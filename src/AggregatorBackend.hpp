/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_AGGREGATOR_BACKEND_HPP
#define __SONATA_AGGREGATOR_BACKEND_HPP

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

class AggregatorBackend : public Backend {

public:
  AggregatorBackend(std::unique_ptr<Backend> &&inner, const tl::pool &pool,
              bool flush_on_read, bool flush_on_exec,
              size_t batch_size, bool commit_on_flush)
      : m_db(std::move(inner)), m_pool(pool), m_flush_on_read(flush_on_read),
        m_flush_on_exec(flush_on_exec), m_batch_size(batch_size),
        m_commit_on_flush(commit_on_flush) {}

  AggregatorBackend(AggregatorBackend &&) = delete;

  AggregatorBackend(const AggregatorBackend &) = delete;

  AggregatorBackend &operator=(AggregatorBackend &&) = delete;

  AggregatorBackend &operator=(const AggregatorBackend &) = delete;

  static std::unique_ptr<Backend> create(const tl::engine &engine,
                                         const tl::pool &pool,
                                         const json &config);

  static std::unique_ptr<Backend> attach(const tl::engine &engine,
                                         const tl::pool &pool,
                                         const json &config);

  virtual ~AggregatorBackend() {}

  virtual RequestResult<bool>
  createCollection(const std::string &coll_name) override {
    auto result = m_db->createCollection(coll_name);
    if(result.success()) {
       std::unique_lock<tl::mutex> lock(m_batches_mtx);
       auto& batches = m_batches[coll_name];
       (void)batches;
    }
    return result;
  }

  virtual RequestResult<bool>
  openCollection(const std::string &coll_name) override {
    auto result = m_db->openCollection(coll_name);
    if(result.success()) {
       std::unique_lock<tl::mutex> lock(m_batches_mtx);
       auto& batches = m_batches[coll_name];
       (void)batches;
    }
    return result;
  }

  virtual RequestResult<bool>
  dropCollection(const std::string &coll_name) override {
    flush();
    auto result = m_db->dropCollection(coll_name);
    if(result.success()) {
       std::unique_lock<tl::mutex> lock(m_batches_mtx);
       m_batches.erase(coll_name);
    }
    return result;
  }

  virtual RequestResult<uint64_t> store(const std::string &coll_name,
                                        const std::string &record,
                                        bool commit) override {
    RequestResult<uint64_t> result;
    JsonWrapper json_record;
    try {
        json_record = json::parse(record);
    } catch(const std::exception& ex) {
      result.error() = ex.what();
      result.success() = false;
      return result;
    }
    return storeJson(coll_name, json_record, commit);
  }

  virtual RequestResult<uint64_t> storeJson(const std::string &coll_name,
                                            const JsonWrapper &record,
                                            bool commit) override {
    RequestResult<uint64_t> result;
    result.success() = true;
    result.value() = std::numeric_limits<uint64_t>::max();
    std::unique_lock<tl::mutex> lock(m_batches_mtx);
    auto it = m_batches.find(coll_name);
    if(it == m_batches.end()) {
        lock.unlock();
        auto coll_exists = openCollection(coll_name);
        if(coll_exists.success()) {
            lock.lock();
            it = m_batches.find(coll_name);
        } else {
            result.success() = false;
            result.error() = "Collection does not exist";
            return result;
        }
    }
    auto& batch = it->second;
    batch.m_content.m_object.push_back(std::move(record.m_object));
    if((batch.m_content.m_object.size() >= m_batch_size) || commit) {
      auto batch_content = std::move(batch.m_content);
      batch.m_content.m_object = json::array();
      m_pool.make_thread(
        [backend = this, coll_name, content = std::move(batch_content), commit]() {
            PendingWrite pw(*backend);
            backend->m_db->storeMultiJson(coll_name, content, commit || backend->m_commit_on_flush);
        }, tl::anonymous());
    }
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
      wrapper->push_back(std::move(t));
    }
    return storeMultiJson(coll_name, wrapper, commit);
  }

  virtual RequestResult<bool> commit() override {
    flush();
    return m_db->commit();
  }

  virtual RequestResult<std::vector<uint64_t>>
  storeMultiJson(const std::string &coll_name, const JsonWrapper &records,
                 bool commit) override {
    RequestResult<std::vector<uint64_t>> result;
    if (!records->is_array()) {
      result.error() = "JSON object is not an array";
      result.success() = false;
      return result;
    }
    result.success() = true;
    result.value() = std::vector<uint64_t>(
        records->size(), std::numeric_limits<uint64_t>::max());

    std::unique_lock<tl::mutex> lock(m_batches_mtx);
    auto it = m_batches.find(coll_name);
    if(it == m_batches.end()) {
        lock.unlock();
        auto coll_exists = openCollection(coll_name);
        if(coll_exists.success()) {
            lock.lock();
            it = m_batches.find(coll_name);
        } else {
            result.success() = false;
            result.error() = "Collection does not exist";
            return result;
        }
    }

    auto& batch = it->second;
    batch.m_content.m_object.insert(
        batch.m_content.m_object.end(),
        records.m_object.begin(),
        records.m_object.end());

    if((batch.m_content.m_object.size() >= m_batch_size) || commit) {
      auto batch_content = std::move(batch.m_content);
      batch.m_content.m_object = json::array();
      m_pool.make_thread(
        [backend = this, coll_name, content = std::move(batch_content), commit]() {
            PendingWrite pw(*backend);
            backend->m_db->storeMultiJson(coll_name, content, commit || backend->m_commit_on_flush);
        }, tl::anonymous());
    }
    return result;
  }

  virtual RequestResult<std::string> fetch(const std::string &coll_name,
                                           uint64_t record_id) override {
    if (m_flush_on_read)
      flush(coll_name);
    return m_db->fetch(coll_name, record_id);
  }

  virtual RequestResult<JsonWrapper> fetchJson(const std::string &coll_name,
                                               uint64_t record_id) override {
    if (m_flush_on_read)
      flush(coll_name);
    return m_db->fetchJson(coll_name, record_id);
  }

  virtual RequestResult<std::vector<std::string>>
  fetchMulti(const std::string &coll_name,
             const std::vector<uint64_t> &record_ids) override {
    if (m_flush_on_read)
      flush(coll_name);
    return m_db->fetchMulti(coll_name, record_ids);
  }

  virtual RequestResult<JsonWrapper>
  fetchMultiJson(const std::string &coll_name,
                 const std::vector<uint64_t> &record_ids) override {
    if (m_flush_on_read)
      flush(coll_name);
    return m_db->fetchMultiJson(coll_name, record_ids);
  }

  virtual RequestResult<std::vector<std::string>>
  filter(const std::string &coll_name,
         const std::string &filter_code) override {
    if (m_flush_on_read)
      flush(coll_name);
    return m_db->filter(coll_name, filter_code);
  }

  virtual RequestResult<JsonWrapper>
  filterJson(const std::string &coll_name,
             const std::string &filter_code) override {
    if (m_flush_on_read)
      flush(coll_name);
    return m_db->filterJson(coll_name, filter_code);
  }

  virtual RequestResult<bool> update(const std::string &coll_name,
                                     uint64_t record_id,
                                     const std::string &new_content,
                                     bool commit) override {
    if (m_flush_on_read)
      flush(coll_name);
    return m_db->update(coll_name, record_id, new_content, commit);
  }

  virtual RequestResult<bool> updateJson(const std::string &coll_name,
                                         uint64_t record_id,
                                         const JsonWrapper &new_content,
                                         bool commit) override {
    if (m_flush_on_read)
      flush(coll_name);
    return m_db->updateJson(coll_name, record_id, new_content, commit);
  }

  virtual RequestResult<std::vector<bool>> updateMulti(
      const std::string &coll_name, const std::vector<uint64_t> &record_ids,
      const std::vector<std::string> &new_contents, bool commit) override {
    if (m_flush_on_read)
      flush(coll_name);
    return m_db->updateMulti(coll_name, record_ids, new_contents, commit);
  }

  virtual RequestResult<std::vector<bool>>
  updateMultiJson(const std::string &coll_name,
                  const std::vector<uint64_t> &record_ids,
                  const JsonWrapper &new_contents, bool commit) override {
    if (m_flush_on_read)
      flush(coll_name);
    return m_db->updateMultiJson(coll_name, record_ids, new_contents, commit);
  }

  virtual RequestResult<std::vector<std::string>>
  all(const std::string &coll_name) override {
    if (m_flush_on_read)
      flush(coll_name);
    return m_db->all(coll_name);
  }

  virtual RequestResult<JsonWrapper>
  allJson(const std::string &coll_name) override {
    if (m_flush_on_read)
      flush(coll_name);
    return m_db->allJson(coll_name);
  }

  virtual RequestResult<uint64_t>
  lastID(const std::string &coll_name) override {
    if (m_flush_on_read)
      flush(coll_name);
    return m_db->lastID(coll_name);
  }

  virtual RequestResult<size_t> size(const std::string &coll_name) override {
    if (m_flush_on_read)
      flush(coll_name);
    return m_db->size(coll_name);
  }

  virtual RequestResult<bool> erase(const std::string &coll_name,
                                    uint64_t record_id, bool commit) override {
    if (m_flush_on_read)
      flush(coll_name);
    return m_db->erase(coll_name, record_id, commit);
  }

  virtual RequestResult<bool>
  eraseMulti(const std::string &coll_name,
             const std::vector<uint64_t> &record_ids, bool commit) override {
    if (m_flush_on_read)
      flush(coll_name);
    return m_db->eraseMulti(coll_name, record_ids, commit);
  }

  virtual RequestResult<std::unordered_map<std::string, std::string>>
  execute(const std::string &code, const std::unordered_set<std::string> &vars,
          bool commit) override {
    if (m_flush_on_exec)
      flush();
    return m_db->execute(code, vars, commit);
  }

  virtual RequestResult<bool> destroy() override {
    flush();
    return m_db->destroy();
  }

  std::string getConfig() const override {
    return "{\"flush_on_exec\":"s + (m_flush_on_exec ? "true" : "false") +
           ",\"flush_on_read\":"s + (m_flush_on_read ? "true" : "false") +
           ",\"commit_on_flush\":"s + (m_commit_on_flush ? "true" : "false") +
           ",\"batch_size\":"s + std::to_string(m_batch_size) +
           ",\"config\":" + m_db->getConfig() + "}";
  }

private:
  void flush(const std::string &coll_name = "") {
    std::unique_lock<tl::mutex> lock(m_batches_mtx);
    if (m_pending_writes == 0)
      return;
    m_batches_cv.wait(lock, [this]() { return m_pending_writes == 0; });
    if(coll_name.empty()) {
      for(auto& p : m_batches) {
        auto& coll = p.first;
        auto& batch = p.second;
        if(batch.m_content.m_object.empty())
          continue;
        m_db->storeMultiJson(coll, batch.m_content, m_commit_on_flush);
        batch.m_content.m_object.clear();
      }
    } else {
      auto it = m_batches.find(coll_name);
      if(it == m_batches.end())
        return;
      auto& batch = it->second;
      if(batch.m_content.m_object.empty())
        return;
      m_db->storeMultiJson(coll_name, batch.m_content, m_commit_on_flush);
      batch.m_content.m_object.clear();
    }
  }

  struct Batch {
    JsonWrapper m_content;
    Batch() { m_content.m_object = json::array(); }
  };

  bool m_flush_on_read = true;
  bool m_flush_on_exec = true;
  bool m_commit_on_flush = true;
  size_t m_batch_size = 32;

  std::unique_ptr<Backend> m_db;
  tl::pool m_pool;
  std::unordered_map<std::string, Batch> m_batches;
  tl::mutex m_batches_mtx;
  tl::condition_variable m_batches_cv;
  uint64_t m_pending_writes = 0;

  struct PendingWrite {

    AggregatorBackend &m_backend;

    PendingWrite(AggregatorBackend &backend) : m_backend(backend) {
      std::unique_lock<tl::mutex> lock(m_backend.m_batches_mtx);
      m_backend.m_pending_writes += 1;
    }

    ~PendingWrite() {
      bool notify = false;
      {
        std::unique_lock<tl::mutex> lock(m_backend.m_batches_mtx);
        m_backend.m_pending_writes -= 1;
        notify = m_backend.m_pending_writes == 0;
      }
      if (notify)
        m_backend.m_batches_cv.notify_all();
    }
  };
};

} // namespace sonata
#endif
