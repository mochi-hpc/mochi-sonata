/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_JSONCPP_BACKEND_HPP
#define __SONATA_JSONCPP_BACKEND_HPP
#include "sonata/Client.hpp"
#include "sonata/Admin.hpp"
#include "sonata/Backend.hpp"

#include <spdlog/spdlog.h>
#include <json/json.h>
#include <cstdio>
#include <sstream>
#include <fstream>
#include <thallium.hpp>

namespace sonata {

namespace tl = thallium;
using namespace std::string_literals;

class JsonCppBackend : public Backend {

    public:

    JsonCppBackend() {}

    JsonCppBackend(JsonCppBackend&&) = delete;

    JsonCppBackend(const JsonCppBackend&) = delete;

    JsonCppBackend& operator=(JsonCppBackend&&) = delete;

    JsonCppBackend& operator=(const JsonCppBackend&) = delete;

    static std::unique_ptr<Backend> create(
            thallium::engine& engine, const Json::Value& config);
    
    static std::unique_ptr<Backend> attach(
            thallium::engine& engine, const Json::Value& config);

    virtual ~JsonCppBackend() {}

    virtual RequestResult<bool> createCollection(
            const std::string& coll_name) override {
        RequestResult<bool> result;
        std::lock_guard<tl::mutex> guard(m_mutex);
        if(m_collections.count(coll_name)) {
            result.success() = false;
            result.error() = "Collection already exists";
        } else {
            m_collections.emplace(coll_name,
                    Json::Value(Json::ValueType::arrayValue));
            m_collection_size.emplace(coll_name, 0);
            result.success() = true;
        }
        return result;
    }

    virtual RequestResult<bool> openCollection(
            const std::string& coll_name) override {
        RequestResult<bool> result;
        std::lock_guard<tl::mutex> guard(m_mutex);
        if(m_collections.count(coll_name)) {
            result.success() = true;
        } else {
            result.error() = "Collection does not exist";
            result.success() = false;
        }
        return result;
    }

    virtual RequestResult<bool> dropCollection(
            const std::string& coll_name) override {
        RequestResult<bool> result;
        std::lock_guard<tl::mutex> guard(m_mutex);
        if(m_collections.count(coll_name)) {
            m_collections.erase(coll_name);
            m_collection_size.erase(coll_name);
            result.success() = true;
        } else {
            result.error() = "Collection does not exist";
            result.success() = false;
        }
        return result;
    }

    virtual RequestResult<uint64_t> store(
            const std::string& coll_name,
            const std::string& record,
            bool commit) override {
        RequestResult<uint64_t> result;
        Json::CharReaderBuilder builder;
        Json::CharReader* reader = builder.newCharReader();
        Json::Value json;
        std::string errors;
        bool parsingSuccessful = reader->parse(
                    record.c_str(),
                    record.c_str() + record.size(),
                    &json,
                    &errors);
        delete reader;
        if(!parsingSuccessful) {
            result.error() = "Invalid JSON record";
            result.success() = false;
            return result;
        } else {
            return storeJson(coll_name, json, commit);
        }
    }

    virtual RequestResult<uint64_t> storeJson(
            const std::string& coll_name,
            const Json::Value& record,
            bool commit) override {
        RequestResult<uint64_t> result;
        std::lock_guard<tl::mutex> guard(m_mutex);
        if(m_collections.count(coll_name) == 0) {
            result.success() = false;
            result.error() = "Collection does not exist";
            return result;
        }
        auto& collection = m_collections[coll_name];
        if(!record.isObject()) {
            result.error() = "JSON object is not an object";
            result.success() = false;
            return result;
        }
        collection.append(record);
        collection[Json::ArrayIndex(collection.size()-1)]["__id"]
            = collection.size()-1;
        m_collection_size[coll_name] += 1;
        result.success() = true;
        result.value() = collection.size()-1;
        return result;
    }
    
    virtual RequestResult<std::vector<uint64_t>> storeMulti(
            const std::string& coll_name,
            const std::vector<std::string>& records,
            bool commit) override {
        RequestResult<std::vector<uint64_t>> result;
        Json::CharReaderBuilder builder;
        Json::CharReader* reader = builder.newCharReader();
        std::string errors;
        Json::Value records_json;
        for(auto& r : records) {
            Json::Value json;
            bool parsingSuccessful = reader->parse(
                    r.c_str(),
                    r.c_str() + r.size(),
                    &json,
                    &errors);
            if(!parsingSuccessful) {
                result.error() = "Invalid JSON record";
                result.success() = false;
                delete reader;
                return result;
            }
            records_json.append(json);
        }
        return storeMultiJson(coll_name, records_json, commit);
    }
    
    virtual RequestResult<bool> commit() override {
        RequestResult<bool> result;
        result.success() = true;
        return result;
    }

    virtual RequestResult<std::vector<uint64_t>> storeMultiJson(
            const std::string& coll_name,
            const Json::Value& records,
            bool commit) override {
        RequestResult<std::vector<uint64_t>> result;
        std::lock_guard<tl::mutex> guard(m_mutex);
        if(m_collections.count(coll_name) == 0) {
            result.success() = false;
            result.error() = "Collection does not exist";
            return result;
        }
        auto& collection = m_collections[coll_name];
        if(!records.isArray()) {
            result.error() = "JSON object is not an array";
            result.success() = false;
            return result;
        }
        auto id = collection.size();
        for(Json::ArrayIndex i = 0; i < records.size(); i++) {
            collection.append(records[i]);
            collection[id + i]["__id"] = id + i;
            result.value().push_back(id + i);
        }
        m_collection_size[coll_name] += records.size();
        return result;
    }

    virtual RequestResult<std::string> fetch(
            const std::string& coll_name,
            uint64_t record_id) override {
        RequestResult<std::string> result;
        auto result_json = fetchJson(coll_name, record_id);
        if(result_json.success()) {
            result.success() = true;
            result.value() = result_json.value().toStyledString();
        } else {
            result.success() = false;
            result.error() = std::move(result_json.error());
        }
        return result;
    }

    virtual RequestResult<Json::Value> fetchJson(
            const std::string& coll_name,
            uint64_t record_id) override {
        RequestResult<Json::Value> result;
        std::lock_guard<tl::mutex> guard(m_mutex);
        if(m_collections.count(coll_name) == 0) {
            result.success() = false;
            result.error() = "Collection does not exist";
            return result;
        }
        auto& collection = m_collections[coll_name];
        if(collection.size() <= record_id) {
            result.success() = false;
            result.error() = "Record id out of range";
            return result;
        }
        auto& record = collection[Json::ArrayIndex(record_id)];
        if(record.type() == Json::nullValue) {
            result.success() = false;
            result.error() = "Record has been erased";
            return result;
        }
        result.success() = true;
        result.value() = record;
        return result;
    }

    virtual RequestResult<std::vector<std::string>> fetchMulti(
            const std::string& coll_name,
            const std::vector<uint64_t>& record_ids) override {
        RequestResult<std::vector<std::string>> result;
        std::lock_guard<tl::mutex> guard(m_mutex);
        if(m_collections.count(coll_name) == 0) {
            result.success() = false;
            result.error() = "Collection does not exist";
            return result;
        }
        auto& collection = m_collections[coll_name];
        result.value().reserve(record_ids.size());
        for(auto id : record_ids) {
            if(id >= collection.size())
                continue;
            const auto& record = collection[Json::ArrayIndex(id)];
            result.value().push_back(record.toStyledString());
        }
        return result;
    }

    virtual RequestResult<Json::Value> fetchMultiJson(
            const std::string& coll_name,
            const std::vector<uint64_t>& record_ids) override {
        RequestResult<Json::Value> result;
        std::lock_guard<tl::mutex> guard(m_mutex);
        if(m_collections.count(coll_name) == 0) {
            result.success() = false;
            result.error() = "Collection does not exist";
            return result;
        }
        auto& collection = m_collections[coll_name];
        for(auto id : record_ids) {
            if(id >= collection.size())
                continue;
            const auto& record = collection[Json::ArrayIndex(id)];
            result.value().append(record);
        }
        return result;
    }

    virtual RequestResult<std::vector<std::string>> filter(
            const std::string& coll_name,
            const std::string& filter_code) override {
        RequestResult<std::vector<std::string>> result;
        result.success() = false;
        result.error() = "Function not implemented with JsonCpp backend";
        return result;
    }

    virtual RequestResult<Json::Value> filterJson(
            const std::string& coll_name,
            const std::string& filter_code) override {
        RequestResult<Json::Value> result;
        result.success() = false;
        result.error() = "Function not implemented with JsonCpp backend";
        return result;
    }

    virtual RequestResult<bool> update(
            const std::string& coll_name,
            uint64_t record_id,
            const std::string& new_content,
            bool commit) override {
        Json::CharReaderBuilder builder;
        Json::CharReader* reader = builder.newCharReader();
        Json::Value json;
        std::string errors;
        bool parsingSuccessful = reader->parse(
                    new_content.c_str(),
                    new_content.c_str() + new_content.size(),
                    &json,
                    &errors);
        delete reader;
        if(!parsingSuccessful) {
            RequestResult<bool> result;
            result.error() = "Invalid JSON record";
            result.success() = false;
            return result;
        } else {
            return updateJson(coll_name, record_id, json, commit);
        }
    }

    virtual RequestResult<bool> updateJson(
            const std::string& coll_name,
            uint64_t record_id,
            const Json::Value& new_content,
            bool commit) override {
        RequestResult<bool> result;
        std::lock_guard<tl::mutex> guard(m_mutex);
        if(m_collections.count(coll_name) == 0) {
            result.success() = false;
            result.error() = "Collection does not exist";
            return result;
        }
        auto& collection = m_collections[coll_name];
        if(record_id >= collection.size()) {
            result.success() = false;
            result.error() = "Record id out of range";
            return result;
        }
        auto& record = collection[Json::ArrayIndex(record_id)];
        if(record.type() == Json::nullValue) {
            result.success() = false;
            result.error() = "Attempt to update an erased record";
            return result;
        }
        record = new_content;
        record["__id"] = record_id;
        result.success() = true;
        return result;
    }

    virtual RequestResult<std::vector<bool>> updateMulti(
            const std::string& coll_name,
            const std::vector<uint64_t>& record_ids,
            const std::vector<std::string>& new_contents,
            bool commit) override {
        RequestResult<std::vector<bool>> result;
        std::lock_guard<tl::mutex> guard(m_mutex);
        if(m_collections.count(coll_name) == 0) {
            result.success() = false;
            result.error() = "Collection does not exist";
            return result;
        }
        auto& collection = m_collections[coll_name];
        Json::CharReaderBuilder builder;
        Json::CharReader* reader = builder.newCharReader();
        std::string errors;
        Json::Value records_json;
        result.value().reserve(record_ids.size());
        for(size_t i = 0; i < record_ids.size(); i++) {
            if(new_contents.size() <= i) {
                result.value().push_back(false);
                continue;
            }
            auto& r = new_contents[i];
            Json::Value json;
            bool parsingSuccessful = reader->parse(
                    r.c_str(),
                    r.c_str() + r.size(),
                    &json,
                    &errors);
            if(!parsingSuccessful) {
                result.value().push_back(false);
            } else {
                auto id = record_ids[i];
                if(collection.size() <= id) {
                    result.value().push_back(false);
                    continue;
                }
                auto& record = collection[Json::ArrayIndex(id)];
                if(record.type() != Json::objectValue) {
                    result.value().push_back(false); 
                    continue;
                }
                record = json;
                record["__id"] = id;
                result.value().push_back(true);
            }
        }
        delete reader;
        return result;
    }

    virtual RequestResult<std::vector<bool>> updateMultiJson(
            const std::string& coll_name,
            const std::vector<uint64_t>& record_ids,
            const Json::Value& new_contents,
            bool commit) override {
        RequestResult<std::vector<bool>> result;
        std::lock_guard<tl::mutex> guard(m_mutex);
        if(m_collections.count(coll_name) == 0) {
            result.success() = false;
            result.error() = "Collection does not exist";
            return result;
        }
        auto& collection = m_collections[coll_name];
        result.value().reserve(record_ids.size());
        for(size_t i=0; i < record_ids.size(); i++) {
            if(i >= new_contents.size()) {
                result.value().push_back(false);
                continue;
            }
            auto id = record_ids[i];
            if(collection.size() <= id) {
                result.value().push_back(false);
                continue;
            }
            auto& record = collection[Json::ArrayIndex(id)];
            if(record.type() != Json::objectValue) {
                result.value().push_back(false); 
                continue;
            }
            record = new_contents[Json::ArrayIndex(i)];
            record["__id"] = id;
            result.value().push_back(true);
        }
        return result;
    }

    virtual RequestResult<std::vector<std::string>> all(
            const std::string& coll_name) override {
        RequestResult<std::vector<std::string>> result;
        std::lock_guard<tl::mutex> guard(m_mutex);
        if(m_collections.count(coll_name) == 0) {
            result.success() = false;
            result.error() = "Collection does not exist";
            return result;
        }
        auto& collection = m_collections[coll_name];
        for(auto& r : collection) {
            if(r.type() != Json::nullValue)
                result.value().push_back(r.toStyledString());
        }
        return result;
    }

    virtual RequestResult<Json::Value> allJson(
            const std::string& coll_name) override {
        RequestResult<Json::Value> result;
        std::lock_guard<tl::mutex> guard(m_mutex);
        if(m_collections.count(coll_name) == 0) {
            result.success() = false;
            result.error() = "Collection does not exist";
            return result;
        }
        auto& collection = m_collections[coll_name];
        for(auto& r : collection) {
            if(r.type() != Json::nullValue)
                result.value().append(r);
        }
        return result;
    }

    virtual RequestResult<uint64_t> lastID(
            const std::string& coll_name) override {
        RequestResult<uint64_t> result;
        std::lock_guard<tl::mutex> guard(m_mutex);
        if(m_collections.count(coll_name) == 0) {
            result.success() = false;
            result.error() = "Collection does not exist";
            return result;
        }
        auto& collection = m_collections[coll_name];
        if(collection.empty()) {
            result.success() = false;
            result.error() = "Empty collection";
            return result;
        }
        result.value() = collection.size()-1;
        return result;
    }

    virtual RequestResult<size_t> size(
            const std::string& coll_name) override {
        std::lock_guard<tl::mutex> guard(m_mutex);
        RequestResult<size_t> result;
        if(m_collections.count(coll_name) == 0) {
            result.success() = false;
            result.error() = "Collection does not exist";
            return result;
        }
        result.value() = m_collection_size[coll_name];
        return result;
    }

    virtual RequestResult<bool> erase(
            const std::string& coll_name,
            uint64_t record_id,
            bool commit) override {
        std::lock_guard<tl::mutex> guard(m_mutex);
        RequestResult<bool> result;
        if(m_collections.count(coll_name) == 0) {
            result.success() = false;
            result.error() = "Collection does not exist";
            return result;
        }
        auto& collection = m_collections[coll_name];
        if(record_id >= collection.size()) {
            result.success() = false;
            result.error() = "Invalid record id";
            return result;
        }
        auto& r = collection[(Json::ArrayIndex)record_id];
        if(r.type() == Json::ValueType::nullValue) {
            result.success() = false;
            result.error() = "Record already erased";
            return result;
        }
        m_collection_size[coll_name] -= 1;
        r = Json::Value::null;
        return result;
    }

    virtual RequestResult<bool> eraseMulti(
            const std::string& coll_name,
            const std::vector<uint64_t>& record_ids,
            bool commit) override {
        RequestResult<bool> result;
        std::lock_guard<tl::mutex> guard(m_mutex);
        if(m_collections.count(coll_name) == 0) {
            result.success() = false;
            result.error() = "Collection does not exist";
            return result;
        }
        auto& collection = m_collections[coll_name];
        auto& size = m_collection_size[coll_name];
        for(auto& id : record_ids) {
            if(id < collection.size()) {
                collection[(Json::ArrayIndex)id] = Json::Value::null;
                size -= 1;
            }
        }
        return result;
    }

    virtual RequestResult<std::unordered_map<std::string,std::string>>
        execute(const std::string& code,
                const std::unordered_set<std::string>& vars,
                bool commit) override {
        RequestResult<std::unordered_map<std::string,std::string>> result;
        result.success() = false;
        result.error() = "Function not implemented for JsonCpp backend";
        return result;
    }

    virtual RequestResult<bool> destroy() override {
        RequestResult<bool> result;
        result.value() = true;
        m_collections.clear();
        m_collection_size.clear();
        return result;
    }

    private:

    std::unordered_map<std::string, Json::Value> m_collections;
    std::unordered_map<std::string, size_t> m_collection_size;
    tl::mutex m_mutex;
};

}
#endif
