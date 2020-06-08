/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_NULL_BACKEND_HPP
#define __SONATA_NULL_BACKEND_HPP
#include "sonata/Client.hpp"
#include "sonata/Admin.hpp"
#include "sonata/Backend.hpp"

#include <spdlog/spdlog.h>
#include <json/json.h>
#include <cstdio>
#include <thallium.hpp>

namespace sonata {

namespace tl = thallium;
using namespace std::string_literals;

class NullBackend : public Backend {

    public:

    NullBackend() {}

    NullBackend(NullBackend&&) = delete;

    NullBackend(const NullBackend&) = delete;

    NullBackend& operator=(NullBackend&&) = delete;

    NullBackend& operator=(const NullBackend&) = delete;

    static std::unique_ptr<Backend> create(
            const thallium::engine& engine, const Json::Value& config);
    
    static std::unique_ptr<Backend> attach(
            const thallium::engine& engine, const Json::Value& config);

    virtual ~NullBackend() {}

    virtual RequestResult<bool> createCollection(
            const std::string& coll_name) override {
        RequestResult<bool> result;
        result.success() = true;
        return result;
    }

    virtual RequestResult<bool> openCollection(
            const std::string& coll_name) override {
        RequestResult<bool> result;
        result.success() = true;
        return result;
    }

    virtual RequestResult<bool> dropCollection(
            const std::string& coll_name) override {
        RequestResult<bool> result;
        result.success() = true;
        return result;
    }

    virtual RequestResult<uint64_t> store(
            const std::string& coll_name,
            const std::string& record,
            bool commit) override {
        RequestResult<uint64_t> result;
        result.value() = 0;
        result.success() = true;
        return result;
    }

    virtual RequestResult<uint64_t> storeJson(
            const std::string& coll_name,
            const Json::Value& record,
            bool commit) override {
        RequestResult<uint64_t> result;
        result.value() = 0;
        result.success() = true;
        return result;
    }
    
    virtual RequestResult<std::vector<uint64_t>> storeMulti(
            const std::string& coll_name,
            const std::vector<std::string>& records,
            bool commit) override {
        RequestResult<std::vector<uint64_t>> result;
        result.value().resize(records.size());
        for(unsigned i=0; i<records.size(); i++)
            result.value()[i] = i;
        result.success() = true;
        return result;
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
        result.value().resize(records.size());
        for(unsigned i=0; i<records.size(); i++)
            result.value()[i] = i;
        result.success() = true;
        return result;
    }

    virtual RequestResult<std::string> fetch(
            const std::string& coll_name,
            uint64_t record_id) override {
        RequestResult<std::string> result;
        result.value() = "{}";
        result.success() = true;
        return result;
    }

    virtual RequestResult<Json::Value> fetchJson(
            const std::string& coll_name,
            uint64_t record_id) override {
        RequestResult<Json::Value> result;
        result.success() = true;
        return result;
    }

    virtual RequestResult<std::vector<std::string>> fetchMulti(
            const std::string& coll_name,
            const std::vector<uint64_t>& record_ids) override {
        RequestResult<std::vector<std::string>> result;
        result.value().resize(record_ids.size(), "{}");
        result.success() = true;
        return result;
    }

    virtual RequestResult<Json::Value> fetchMultiJson(
            const std::string& coll_name,
            const std::vector<uint64_t>& record_ids) override {
        RequestResult<Json::Value> result;
        result.value().resize(record_ids.size());
        result.success() = true;
        return result;
    }

    virtual RequestResult<std::vector<std::string>> filter(
            const std::string& coll_name,
            const std::string& filter_code) override {
        RequestResult<std::vector<std::string>> result;
        result.success() = true;
        return result;
    }

    virtual RequestResult<Json::Value> filterJson(
            const std::string& coll_name,
            const std::string& filter_code) override {
        RequestResult<Json::Value> result;
        result.success() = true;
        return result;
    }

    virtual RequestResult<bool> update(
            const std::string& coll_name,
            uint64_t record_id,
            const std::string& new_content,
            bool commit) override {
        RequestResult<bool> result;
        result.success() = true;
        return result;
    }

    virtual RequestResult<bool> updateJson(
            const std::string& coll_name,
            uint64_t record_id,
            const Json::Value& new_content,
            bool commit) override {
        RequestResult<bool> result;
        result.success() = true;
        return result;
    }

    virtual RequestResult<std::vector<bool>> updateMulti(
            const std::string& coll_name,
            const std::vector<uint64_t>& record_ids,
            const std::vector<std::string>& new_contents,
            bool commit) override {
        RequestResult<std::vector<bool>> result;
        result.value().resize(record_ids.size(), true);
        result.success() = true;
        return result;
    }

    virtual RequestResult<std::vector<bool>> updateMultiJson(
            const std::string& coll_name,
            const std::vector<uint64_t>& record_ids,
            const Json::Value& new_contents,
            bool commit) override {
        RequestResult<std::vector<bool>> result;
        result.value().resize(record_ids.size(), true);
        result.success() = true;
        return result;
    }

    virtual RequestResult<std::vector<std::string>> all(
            const std::string& coll_name) override {
        RequestResult<std::vector<std::string>> result;
        result.success() = true;
        return result;
    }

    virtual RequestResult<Json::Value> allJson(
            const std::string& coll_name) override {
        RequestResult<Json::Value> result;
        result.success() = true;
        return result;
    }

    virtual RequestResult<uint64_t> lastID(
            const std::string& coll_name) override {
        RequestResult<uint64_t> result;
        result.value() = 0;
        result.success() = true;
        return result;
    }

    virtual RequestResult<size_t> size(
            const std::string& coll_name) override {
        RequestResult<uint64_t> result;
        result.value() = 0;
        result.success() = true;
        return result;
    }

    virtual RequestResult<bool> erase(
            const std::string& coll_name,
            uint64_t record_id,
            bool commit) override {
        RequestResult<bool> result;
        result.success() = true;
        return result;
    }

    virtual RequestResult<bool> eraseMulti(
            const std::string& coll_name,
            const std::vector<uint64_t>& record_ids,
            bool commit) override {
        RequestResult<bool> result;
        result.success() = true;
        return result;
    }

    virtual RequestResult<std::unordered_map<std::string,std::string>>
        execute(const std::string& code,
                const std::unordered_set<std::string>& vars,
                bool commit) override {
        RequestResult<std::unordered_map<std::string,std::string>> result;
        for(const auto& v : vars) {
            result.value().emplace(v, "null");
        }
        result.success() = true;
        return result;
    }

    virtual RequestResult<bool> destroy() override {
        RequestResult<bool> result;
        result.success() = true;
        return result;
    }

};

}
#endif
