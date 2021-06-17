/*
 * (C) 2021 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_VECTOR_BACKEND_HPP
#define __SONATA_VECTOR_BACKEND_HPP

#include "sonata/Admin.hpp"
#include "sonata/Backend.hpp"
#include "sonata/Client.hpp"

#include <cstdio>
#include <fstream>
#include <json/json.h>
#include <spdlog/spdlog.h>
#include <sstream>
#include <thallium.hpp>

namespace sonata {

namespace tl = thallium;
using namespace std::string_literals;

class VectorBackend : public Backend {

  using collection_t = std::vector<std::string>;

public:
  VectorBackend() {}

  VectorBackend(VectorBackend &&) = delete;

  VectorBackend(const VectorBackend &) = delete;

  VectorBackend &operator=(VectorBackend &&) = delete;

  VectorBackend &operator=(const VectorBackend &) = delete;

  static std::unique_ptr<Backend> create(const tl::engine &engine,
                                         const tl::pool &pool,
                                         const Json::Value &config);

  static std::unique_ptr<Backend> attach(const tl::engine &engine,
                                         const tl::pool &pool,
                                         const Json::Value &config);

  virtual ~VectorBackend() {}

  virtual RequestResult<bool>
  createCollection(const std::string &coll_name) override {
    RequestResult<bool> result;
    std::lock_guard<tl::mutex> guard(m_mutex);
    if (m_collections.count(coll_name)) {
      result.success() = false;
      result.error() = "Collection already exists";
    } else {
      m_collections[coll_name] = collection_t();
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
    std::lock_guard<tl::mutex> guard(m_mutex);
    if (m_collections.count(coll_name) == 0) {
      result.success() = false;
      result.error() = "Collection does not exist";
      return result;
    }
    auto& collection = m_collections[coll_name];
    auto& coll_size  = m_collection_size[coll_name];
    collection.push_back(record);
    result.value() = coll_size;
    coll_size += 1;
    return result;
  }

  virtual RequestResult<uint64_t> storeJson(const std::string &coll_name,
                                            const Json::Value &record,
                                            bool commit) override {
    return store(coll_name, record.toStyledString(), commit);;
  }

  virtual RequestResult<std::vector<uint64_t>>
  storeMulti(const std::string &coll_name,
             const std::vector<std::string> &records, bool commit) override {
    RequestResult<std::vector<uint64_t>> result;
    std::lock_guard<tl::mutex> guard(m_mutex);
    if (m_collections.count(coll_name) == 0) {
      result.success() = false;
      result.error() = "Collection does not exist";
      return result;
    }
    auto& collection = m_collections[coll_name];
    auto& coll_size  = m_collection_size[coll_name];
    size_t i = collection.size();
    for (auto &r : records) {
        collection.push_back(r);
        result.value().push_back(i);
        coll_size += 1;
        i += 1;
    }
    return result;
  }

  virtual RequestResult<bool> commit() override {
    RequestResult<bool> result;
    result.success() = true;
    return result;
  }

  virtual RequestResult<std::vector<uint64_t>>
  storeMultiJson(const std::string &coll_name, const Json::Value &records,
                 bool commit) override {
    RequestResult<std::vector<uint64_t>> result;
    std::lock_guard<tl::mutex> guard(m_mutex);
    if (m_collections.count(coll_name) == 0) {
      result.success() = false;
      result.error() = "Collection does not exist";
      return result;
    }
    auto &collection = m_collections[coll_name];
    if (!records.isArray()) {
      result.error() = "JSON object is not an array";
      result.success() = false;
      return result;
    }
    auto id = collection.size();
    for (Json::ArrayIndex i = 0; i < records.size(); i++) {
      collection.push_back(records[i].toStyledString());
      result.value().push_back(id + i);
    }
    m_collection_size[coll_name] += records.size();
    return result;
  }

  virtual RequestResult<std::string> fetch(const std::string &coll_name,
                                           uint64_t record_id) override {
    RequestResult<std::string> result;
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
    if (record.empty()) {
      result.success() = false;
      result.error() = "Record has been erased";
      return result;
    }
    result.success() = true;
    result.value() = record;
    return result;
  }

  virtual RequestResult<Json::Value> fetchJson(const std::string &coll_name,
                                               uint64_t record_id) override {
    RequestResult<Json::Value> result;
    auto r = fetch(coll_name, record_id);
    if(r.success()) {
        result.success() = true;
        Json::CharReaderBuilder builder;
        Json::CharReader* reader = builder.newCharReader();
        std::string errors;
        result.success() = reader->parse(r.value().c_str(),
                                         r.value().c_str() + r.value().size(),
                                         &result.value(), &errors);
        if(!result.success())
            result.error() = std::move(errors);
        delete reader;
    } else {
        result.success() = false;
        result.error() = r.error();
    }
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
      if (id >= collection.size()) {
          result.value().emplace_back();
          continue;
      }
      const auto &record = collection[id];
      result.value().push_back(record);
    }
    return result;
  }

  virtual RequestResult<Json::Value>
  fetchMultiJson(const std::string &coll_name,
                 const std::vector<uint64_t> &record_ids) override {
    RequestResult<Json::Value> result;
    auto r = fetchMulti(coll_name, record_ids);
    if(r.success()) {
        Json::CharReaderBuilder builder;
        Json::CharReader* reader = builder.newCharReader();
        std::string errors;
        result.success() = true;
        for(auto& record : r.value()) {
            if(record.empty()) {
                result.value().append(Json::Value::null);
            } else {
                Json::Value j;
                auto b = reader->parse(record.c_str(),
                                       record.c_str() + record.size(),
                                       &j, &errors);
                if(b) {
                    result.value().append(std::move(j));
                } else {
                    result.success() = false;
                    result.value().clear();
                    result.error() = std::move(errors);
                    break;
                }
            }
        }
        delete reader;
    } else {
        result.success() = false;
        result.error() = r.error();
    }
    return result;
  }

  virtual RequestResult<std::vector<std::string>>
  filter(const std::string &coll_name,
         const std::string &filter_code) override {
    RequestResult<std::vector<std::string>> result;
    result.success() = false;
    result.error() = "Function not implemented with Vector backend";
    return result;
  }

  virtual RequestResult<Json::Value>
  filterJson(const std::string &coll_name,
             const std::string &filter_code) override {
    RequestResult<Json::Value> result;
    result.success() = false;
    result.error() = "Function not implemented with Vector backend";
    return result;
  }

  virtual RequestResult<bool> update(const std::string &coll_name,
                                     uint64_t record_id,
                                     const std::string &new_content,
                                     bool commit) override {
    RequestResult<bool> result;
    std::lock_guard<tl::mutex> guard(m_mutex);
    if (m_collections.count(coll_name) == 0) {
      result.success() = false;
      result.error() = "Collection does not exist";
      return result;
    }
    auto &collection = m_collections[coll_name];
    if(record_id >= collection.size()) {
      result.success() = false;
      result.error() = "Invalid record id";
      return result;
    }
    collection[record_id] = new_content;
    return result;
  }

  virtual RequestResult<bool> updateJson(const std::string &coll_name,
                                         uint64_t record_id,
                                         const Json::Value &new_content,
                                         bool commit) override {
    RequestResult<bool> result;
    return update(coll_name, record_id, new_content.toStyledString(), commit);
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
      auto id = record_ids[i];
      if (collection.size() <= id) {
          result.value().push_back(false);
          continue;
      }
      collection[id] = r;
      result.value().push_back(true);
    }
    return result;
  }

  virtual RequestResult<std::vector<bool>>
  updateMultiJson(const std::string &coll_name,
                  const std::vector<uint64_t> &record_ids,
                  const Json::Value &new_contents, bool commit) override {
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
      if (i >= new_contents.size()) {
        result.value().push_back(false);
        continue;
      }
      auto id = record_ids[i];
      if (collection.size() <= id) {
        result.value().push_back(false);
        continue;
      }
      auto &record = collection[id];
      if (new_contents[(Json::ArrayIndex)i].type() != Json::objectValue) {
        result.value().push_back(false);
        continue;
      }
      record = new_contents[(Json::ArrayIndex)i].toStyledString();
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
    result.value().reserve(collection.size());
    for (auto &r : collection) {
      if (!r.empty())
        result.value().push_back(r);
    }
    return result;
  }

  virtual RequestResult<Json::Value>
  allJson(const std::string &coll_name) override {
    RequestResult<Json::Value> result;
    std::lock_guard<tl::mutex> guard(m_mutex);

    if (m_collections.count(coll_name) == 0) {
      result.success() = false;
      result.error() = "Collection does not exist";
      return result;
    }

    auto &collection = m_collections[coll_name];

    Json::CharReaderBuilder builder;
    Json::CharReader* reader = builder.newCharReader();
    std::string errors;

    for (auto &r : collection) {
      if (r.empty())
          continue;
      Json::Value j;
      result.success() = reader->parse(r.c_str(), r.c_str()+r.size(),
                                       &j, &errors);
      if(!result.success()) {
          result.error() = std::move(errors);
          result.value().clear();
          break;
      }
      result.value().append(std::move(j));
    }

    delete reader;
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
    m_collection_size[coll_name] -= 1;
    r.clear();
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
        collection[id].clear();
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
  std::unordered_map<std::string, collection_t> m_collections;
  std::unordered_map<std::string, size_t> m_collection_size;
  tl::mutex m_mutex;
};

} // namespace sonata
#endif
