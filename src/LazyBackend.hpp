/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_LAZY_BACKEND_HPP
#define __SONATA_LAZY_BACKEND_HPP
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

class LazyBackend : public Backend {

public:
  LazyBackend(std::unique_ptr<Backend> &&inner, const tl::pool &pool,
              bool flush_on_read, bool flush_on_exec)
      : m_db(std::move(inner)), m_pool(pool), m_flush_on_read(flush_on_read),
        m_flush_on_exec(flush_on_exec) {}

  LazyBackend(LazyBackend &&) = delete;

  LazyBackend(const LazyBackend &) = delete;

  LazyBackend &operator=(LazyBackend &&) = delete;

  LazyBackend &operator=(const LazyBackend &) = delete;

  static std::unique_ptr<Backend> create(const tl::engine &engine,
                                         const tl::pool &pool,
                                         const Json::Value &config);

  static std::unique_ptr<Backend> attach(const tl::engine &engine,
                                         const tl::pool &pool,
                                         const Json::Value &config);

  virtual ~LazyBackend() {}

  virtual RequestResult<bool>
  createCollection(const std::string &coll_name) override {
    return m_db->createCollection(coll_name);
  }

  virtual RequestResult<bool>
  openCollection(const std::string &coll_name) override {
    return m_db->openCollection(coll_name);
  }

  virtual RequestResult<bool>
  dropCollection(const std::string &coll_name) override {
    flush();
    return m_db->dropCollection(coll_name);
  }

  virtual RequestResult<uint64_t> store(const std::string &coll_name,
                                        const std::string &record,
                                        bool commit) override {
    RequestResult<uint64_t> result;
    Json::CharReaderBuilder builder;
    Json::CharReader *reader = builder.newCharReader();
    Json::Value json;
    std::string errors;
    bool parsingSuccessful = reader->parse(
        record.c_str(), record.c_str() + record.size(), &json, &errors);
    delete reader;
    if (!parsingSuccessful) {
      result.error() = "Invalid JSON record";
      result.success() = false;
      return result;
    } else {
      return storeJson(coll_name, json, commit);
    }
  }

  virtual RequestResult<uint64_t> storeJson(const std::string &coll_name,
                                            const Json::Value &record,
                                            bool commit) override {
    RequestResult<uint64_t> result;
    result.success() = true;
    result.value() = std::numeric_limits<uint64_t>::max();
    // TODO check that collection exists
    m_pool.make_thread(
        [backend = this, coll_name, r = std::move(record), commit]() {
          PendingWrite pw(*backend);
          backend->m_db->storeJson(coll_name, r, commit);
        },
        tl::anonymous());
    return result;
  }

  virtual RequestResult<std::vector<uint64_t>>
  storeMulti(const std::string &coll_name,
             const std::vector<std::string> &records, bool commit) override {
    RequestResult<std::vector<uint64_t>> result;
    Json::CharReaderBuilder builder;
    Json::CharReader *reader = builder.newCharReader();
    std::string errors;
    Json::Value records_json;
    for (auto &r : records) {
      Json::Value json;
      bool parsingSuccessful =
          reader->parse(r.c_str(), r.c_str() + r.size(), &json, &errors);
      if (!parsingSuccessful) {
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
    flush();
    return m_db->commit();
  }

  virtual RequestResult<std::vector<uint64_t>>
  storeMultiJson(const std::string &coll_name, const Json::Value &records,
                 bool commit) override {
    RequestResult<std::vector<uint64_t>> result;
    if (!records.isArray()) {
      result.error() = "JSON object is not an array";
      result.success() = false;
      return result;
    }
    result.success() = true;
    result.value() = std::vector<uint64_t>(
        records.size(), std::numeric_limits<uint64_t>::max());
    // TODO check that collection exists
    m_pool.make_thread(
        [backend = this, coll_name, r = std::move(records), commit]() {
          PendingWrite pw(*backend);
          backend->m_db->storeMultiJson(coll_name, r, commit);
        },
        tl::anonymous());
    return result;
  }

  virtual RequestResult<std::string> fetch(const std::string &coll_name,
                                           uint64_t record_id) override {
    if (m_flush_on_read)
      flush(coll_name);
    return m_db->fetch(coll_name, record_id);
  }

  virtual RequestResult<Json::Value> fetchJson(const std::string &coll_name,
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

  virtual RequestResult<Json::Value>
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

  virtual RequestResult<Json::Value>
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
                                         const Json::Value &new_content,
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
                  const Json::Value &new_contents, bool commit) override {
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

  virtual RequestResult<Json::Value>
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
    return "{\"flush-on-exec\": "s + (m_flush_on_exec ? "true" : "false") +
           ", \"flush-on-read\": "s + (m_flush_on_read ? "true" : "false") +
           ", \"config\": " + m_db->getConfig() + "}";
  }

private:
  void flush(const std::string &coll_name = "") {
    std::unique_lock<tl::mutex> lock(m_pending_writes_mtx);
    if (m_pending_writes == 0)
      return;
    m_pending_writes_cv.wait(lock, [this]() { return m_pending_writes == 0; });
  }

  bool m_flush_on_read = true;
  bool m_flush_on_exec = true;
  std::unique_ptr<Backend> m_db;
  tl::pool m_pool;
  uint64_t m_pending_writes = 0;
  tl::mutex m_pending_writes_mtx;
  tl::condition_variable m_pending_writes_cv;

  struct PendingWrite {

    LazyBackend &m_backend;

    PendingWrite(LazyBackend &backend) : m_backend(backend) {
      std::unique_lock<tl::mutex> lock(m_backend.m_pending_writes_mtx);
      m_backend.m_pending_writes += 1;
    }

    ~PendingWrite() {
      bool notify = false;
      {
        std::unique_lock<tl::mutex> lock(m_backend.m_pending_writes_mtx);
        m_backend.m_pending_writes -= 1;
        notify = m_backend.m_pending_writes == 0;
      }
      if (notify)
        m_backend.m_pending_writes_cv.notify_all();
    }
  };
};

} // namespace sonata
#endif
