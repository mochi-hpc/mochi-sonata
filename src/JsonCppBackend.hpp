/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_JSONCPP_BACKEND_HPP
#define __SONATA_JSONCPP_BACKEND_HPP

#include "sonata/Admin.hpp"
#include "sonata/Backend.hpp"
#include "sonata/Client.hpp"

#include <cstdio>
#include <fstream>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <sstream>
#include <thallium.hpp>

namespace sonata {

namespace tl = thallium;
using namespace std::string_literals;
using nlohmann::json;

class JsonCppBackend : public Backend {

public:
  JsonCppBackend() {}

  JsonCppBackend(JsonCppBackend &&) = delete;

  JsonCppBackend(const JsonCppBackend &) = delete;

  JsonCppBackend &operator=(JsonCppBackend &&) = delete;

  JsonCppBackend &operator=(const JsonCppBackend &) = delete;

  static std::unique_ptr<Backend> create(const tl::engine &engine,
                                         const tl::pool &pool,
                                         const json &config);

  static std::unique_ptr<Backend> attach(const tl::engine &engine,
                                         const tl::pool &pool,
                                         const json &config);

  virtual ~JsonCppBackend() {}

  virtual RequestResult<bool>
  createCollection(const std::string &coll_name) override {
    RequestResult<bool> result;
    std::lock_guard<tl::mutex> guard(m_mutex);
    if (m_collections.count(coll_name)) {
      result.success() = false;
      result.error() = "Collection already exists";
    } else {
      m_collections.emplace(coll_name,
                            json::array());
      m_collection_size.emplace(coll_name, 0);
      result.success() = true;
    }
    return result;
  }

  virtual RequestResult<bool>
  openCollection(const std::string &coll_name) override {
    RequestResult<bool> result;
    std::lock_guard<tl::mutex> guard(m_mutex);
    if (m_collections.count(coll_name)) {
      result.success() = true;
    } else {
      result.error() = "Collection does not exist";
      result.success() = false;
    }
    return result;
  }

  virtual RequestResult<bool>
  dropCollection(const std::string &coll_name) override {
    RequestResult<bool> result;
    std::lock_guard<tl::mutex> guard(m_mutex);
    if (m_collections.count(coll_name)) {
      m_collections.erase(coll_name);
      m_collection_size.erase(coll_name);
      result.success() = true;
    } else {
      result.error() = "Collection does not exist";
      result.success() = false;
    }
    return result;
  }

  virtual RequestResult<uint64_t> store(const std::string &coll_name,
                                        const std::string &record,
                                        bool commit) override {
    RequestResult<uint64_t> result;
    JsonWrapper wrapper;
    try {
        wrapper = json::parse(record);
    } catch(const std::exception& ex) {
      result.error() = ex.what();
      result.success() = false;
      return result;
    }
    return storeJson(coll_name, wrapper, commit);
  }

  virtual RequestResult<uint64_t> storeJson(const std::string &coll_name,
                                            const JsonWrapper &record,
                                            bool commit) override {
    RequestResult<uint64_t> result;
    std::lock_guard<tl::mutex> guard(m_mutex);
    if (m_collections.count(coll_name) == 0) {
      result.success() = false;
      result.error() = "Collection does not exist";
      return result;
    }
    auto &collection = m_collections[coll_name];
    if (!record->is_object()) {
      result.error() = "JSON object is not an object";
      result.success() = false;
      return result;
    }
    collection.push_back(record.m_object);
    collection[collection.size() - 1]["__id"] =
        collection.size() - 1;
    m_collection_size[coll_name] += 1;
    result.success() = true;
    result.value() = collection.size() - 1;
    return result;
  }

  virtual RequestResult<std::vector<uint64_t>>
  storeMulti(const std::string &coll_name,
             const std::vector<std::string> &records, bool commit) override {
    RequestResult<std::vector<uint64_t>> result;
    JsonWrapper wrapper;
    wrapper = json::array();
    for (auto &r : records) {
        json t;
        try {
            t = json::parse(r);
        } catch(const std::exception& ex) {
            result.error() = ex.what();
            result.success() = false;
            return result;
        }
        wrapper->push_back(t);
    }
    return storeMultiJson(coll_name, wrapper, commit);
  }

  virtual RequestResult<bool> commit() override {
    RequestResult<bool> result;
    result.success() = true;
    return result;
  }

  virtual RequestResult<std::vector<uint64_t>>
  storeMultiJson(const std::string &coll_name, const JsonWrapper &records,
                 bool commit) override {
    RequestResult<std::vector<uint64_t>> result;
    std::lock_guard<tl::mutex> guard(m_mutex);
    if (m_collections.count(coll_name) == 0) {
      result.success() = false;
      result.error() = "Collection does not exist";
      return result;
    }
    auto &collection = m_collections[coll_name];
    if (!records->is_array()) {
      result.error() = "JSON object is not an array";
      result.success() = false;
      return result;
    }
    auto id = collection.size();
    auto num_new_records = records->size();
    for (decltype(id) i = 0; i < records->size(); i++) {
      collection.push_back(std::move(records.m_object[i]));
      collection[id + i]["__id"] = id + i;
      result.value().push_back(id + i);
    }
    m_collection_size[coll_name] += num_new_records;
    return result;
  }

  virtual RequestResult<std::string> fetch(const std::string &coll_name,
                                           uint64_t record_id) override {
    RequestResult<std::string> result;
    auto result_json = fetchJson(coll_name, record_id);
    if (result_json.success()) {
      result.success() = true;
      result.value() = result_json.value()->dump();
    } else {
      result.success() = false;
      result.error() = std::move(result_json.error());
    }
    return result;
  }

  virtual RequestResult<JsonWrapper> fetchJson(const std::string &coll_name,
                                               uint64_t record_id) override {
    RequestResult<JsonWrapper> result;
    std::lock_guard<tl::mutex> guard(m_mutex);
    if (m_collections.count(coll_name) == 0) {
      result.success() = false;
      result.error() = "Collection does not exist";
      return result;
    }
    auto &collection = m_collections[coll_name];
    if (collection.size() <= record_id) {
      result.success() = false;
      result.error() = "Record id out of range";
      return result;
    }
    auto &record = collection[record_id];
    if (record.is_null()) {
      result.success() = false;
      result.error() = "Record has been erased";
      return result;
    }
    result.success() = true;
    result.value() = record;
    return result;
  }

  virtual RequestResult<std::vector<std::string>>
  fetchMulti(const std::string &coll_name,
             const std::vector<uint64_t> &record_ids) override {
    RequestResult<std::vector<std::string>> result;
    std::lock_guard<tl::mutex> guard(m_mutex);
    if (m_collections.count(coll_name) == 0) {
      result.success() = false;
      result.error() = "Collection does not exist";
      return result;
    }
    auto &collection = m_collections[coll_name];
    result.value().reserve(record_ids.size());
    for (auto id : record_ids) {
      if (id >= collection.size())
        continue;
      const auto &record = collection[id];
      result.value().push_back(record.dump());
    }
    return result;
  }

  virtual RequestResult<JsonWrapper>
  fetchMultiJson(const std::string &coll_name,
                 const std::vector<uint64_t> &record_ids) override {
    RequestResult<JsonWrapper> result;
    std::lock_guard<tl::mutex> guard(m_mutex);
    if (m_collections.count(coll_name) == 0) {
      result.success() = false;
      result.error() = "Collection does not exist";
      return result;
    }
    auto &collection = m_collections[coll_name];
    result.value() = json::array();
    for (auto id : record_ids) {
      if (id >= collection.size())
        continue;
      const auto &record = collection[id];
      result.value()->push_back(record);
    }
    return result;
  }

  virtual RequestResult<std::vector<std::string>>
  filter(const std::string &coll_name,
         const std::string &filter_code) override {
    RequestResult<std::vector<std::string>> result;
    result.success() = false;
    result.error() = "Function not implemented with JsonCpp backend";
    return result;
  }

  virtual RequestResult<JsonWrapper>
  filterJson(const std::string &coll_name,
             const std::string &filter_code) override {
    RequestResult<JsonWrapper> result;
    result.success() = false;
    result.error() = "Function not implemented with JsonCpp backend";
    return result;
  }

  virtual RequestResult<bool> update(const std::string &coll_name,
                                     uint64_t record_id,
                                     const std::string &new_content,
                                     bool commit) override {
    JsonWrapper wrapper;
    try {
        wrapper = json::parse(new_content);
    } catch(const std::exception& ex) {
      RequestResult<bool> result;
      result.error() = ex.what();
      result.success() = false;
      return result;
    }
    return updateJson(coll_name, record_id, wrapper, commit);
  }

  virtual RequestResult<bool> updateJson(const std::string &coll_name,
                                         uint64_t record_id,
                                         const JsonWrapper &new_content,
                                         bool commit) override {
    RequestResult<bool> result;
    std::lock_guard<tl::mutex> guard(m_mutex);
    if (m_collections.count(coll_name) == 0) {
      result.success() = false;
      result.error() = "Collection does not exist";
      return result;
    }
    auto &collection = m_collections[coll_name];
    if (record_id >= collection.size()) {
      result.success() = false;
      result.error() = "Record id out of range";
      return result;
    }
    auto &record = collection[record_id];
    record = std::move(new_content.m_object);
    record["__id"] = record_id;
    result.success() = true;
    return result;
  }

  virtual RequestResult<std::vector<bool>> updateMulti(
      const std::string &coll_name, const std::vector<uint64_t> &record_ids,
      const std::vector<std::string> &new_contents, bool commit) override {
    RequestResult<std::vector<bool>> result;
    std::lock_guard<tl::mutex> guard(m_mutex);
    if (m_collections.count(coll_name) == 0) {
      result.success() = false;
      result.error() = "Collection does not exist";
      return result;
    }
    auto &collection = m_collections[coll_name];
    result.value().reserve(record_ids.size());
    for (size_t i = 0; i < record_ids.size(); i++) {
      if (new_contents.size() <= i) {
        result.value().push_back(false);
        continue;
      }
      auto &r = new_contents[i];
      json json_r;
      try {
          json_r = json::parse(r);
      } catch(const std::exception& ex) {
        result.value().push_back(false);
      }
      auto id = record_ids[i];
      if (collection.size() <= id) {
          result.value().push_back(false);
          continue;
      }
      auto &record = collection[id];
      if (!record.is_object()) {
          result.value().push_back(false);
          continue;
      }
      record = std::move(json_r);
      record["__id"] = id;
      result.value().push_back(true);
    }
    return result;
  }

  virtual RequestResult<std::vector<bool>>
  updateMultiJson(const std::string &coll_name,
                  const std::vector<uint64_t> &record_ids,
                  const JsonWrapper &new_contents, bool commit) override {
    RequestResult<std::vector<bool>> result;
    std::lock_guard<tl::mutex> guard(m_mutex);
    if (m_collections.count(coll_name) == 0) {
      result.success() = false;
      result.error() = "Collection does not exist";
      return result;
    }
    auto &collection = m_collections[coll_name];
    result.value().reserve(record_ids.size());
    for (size_t i = 0; i < record_ids.size(); i++) {
      if (i >= new_contents->size()) {
        result.value().push_back(false);
        continue;
      }
      auto id = record_ids[i];
      if (collection.size() <= id) {
        result.value().push_back(false);
        continue;
      }
      auto &record = collection[id];
      record = new_contents.m_object[i];
      record["__id"] = id;
      result.value().push_back(true);
    }
    return result;
  }

  virtual RequestResult<std::vector<std::string>>
  all(const std::string &coll_name) override {
    RequestResult<std::vector<std::string>> result;
    std::lock_guard<tl::mutex> guard(m_mutex);
    if (m_collections.count(coll_name) == 0) {
      result.success() = false;
      result.error() = "Collection does not exist";
      return result;
    }
    auto &collection = m_collections[coll_name];
    for (auto &r : collection) {
      if (!r.is_null())
        result.value().push_back(r.dump());
    }
    return result;
  }

  virtual RequestResult<JsonWrapper>
  allJson(const std::string &coll_name) override {
    RequestResult<JsonWrapper> result;
    std::lock_guard<tl::mutex> guard(m_mutex);
    if (m_collections.count(coll_name) == 0) {
      result.success() = false;
      result.error() = "Collection does not exist";
      return result;
    }
    auto &collection = m_collections[coll_name];
    result.value() = collection;
    return result;
  }

  virtual RequestResult<uint64_t>
  lastID(const std::string &coll_name) override {
    RequestResult<uint64_t> result;
    std::lock_guard<tl::mutex> guard(m_mutex);
    if (m_collections.count(coll_name) == 0) {
      result.success() = false;
      result.error() = "Collection does not exist";
      return result;
    }
    auto &collection = m_collections[coll_name];
    if (collection.empty()) {
      result.success() = false;
      result.error() = "Empty collection";
      return result;
    }
    result.value() = collection.size() - 1;
    return result;
  }

  virtual RequestResult<size_t> size(const std::string &coll_name) override {
    std::lock_guard<tl::mutex> guard(m_mutex);
    RequestResult<size_t> result;
    if (m_collections.count(coll_name) == 0) {
      result.success() = false;
      result.error() = "Collection does not exist";
      return result;
    }
    result.value() = m_collection_size[coll_name];
    return result;
  }

  virtual RequestResult<bool> erase(const std::string &coll_name,
                                    uint64_t record_id, bool commit) override {
    std::lock_guard<tl::mutex> guard(m_mutex);
    RequestResult<bool> result;
    if (m_collections.count(coll_name) == 0) {
      result.success() = false;
      result.error() = "Collection does not exist";
      return result;
    }
    auto &collection = m_collections[coll_name];
    if (record_id >= collection.size()) {
      result.success() = false;
      result.error() = "Invalid record id";
      return result;
    }
    auto &r = collection[record_id];
    if (r.is_null()) {
      result.success() = false;
      result.error() = "Record already erased";
      return result;
    }
    m_collection_size[coll_name] -= 1;
    r = json();
    return result;
  }

  virtual RequestResult<bool>
  eraseMulti(const std::string &coll_name,
             const std::vector<uint64_t> &record_ids, bool commit) override {
    RequestResult<bool> result;
    std::lock_guard<tl::mutex> guard(m_mutex);
    if (m_collections.count(coll_name) == 0) {
      result.success() = false;
      result.error() = "Collection does not exist";
      return result;
    }
    auto &collection = m_collections[coll_name];
    auto &size = m_collection_size[coll_name];
    for (auto &id : record_ids) {
      if (id < collection.size()) {
        collection[id] = json();
        size -= 1;
      }
    }
    return result;
  }

  virtual RequestResult<std::unordered_map<std::string, std::string>>
  execute(const std::string &code, const std::unordered_set<std::string> &vars,
          bool commit) override {
    RequestResult<std::unordered_map<std::string, std::string>> result;
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

  std::string getConfig() const override { return "{}"; }

private:
  std::unordered_map<std::string, json> m_collections;
  std::unordered_map<std::string, size_t> m_collection_size;
  tl::mutex m_mutex;
};

} // namespace sonata
#endif
