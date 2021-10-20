/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_PROVIDER_IMPL_H
#define __SONATA_PROVIDER_IMPL_H

#include "sonata/Backend.hpp"
#include "sonata/JsonSerialize.hpp"

#include <thallium.hpp>
#include <thallium/serialization/stl/pair.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/unordered_map.hpp>
#include <thallium/serialization/stl/unordered_set.hpp>
#include <thallium/serialization/stl/vector.hpp>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <tuple>

using nlohmann::json;

#define FIND_DATABASE(__dbvar__)                                               \
  std::shared_ptr<Backend> __dbvar__;                                          \
  do {                                                                         \
      {                                                                        \
        std::lock_guard<tl::mutex> lock(m_backends_mtx);                       \
        auto it = m_backends.find(db_name);                                    \
        if (it != m_backends.end())                                            \
            __dbvar__ = it->second;                                            \
      }                                                                        \
      if (!__dbvar__) {                                                        \
        result.success() = false;                                              \
        result.error() = "Database "s + db_name + " not found";                \
        req.respond(result);                                                   \
        spdlog::error("[provider:{}] Database {} not found", id(), db_name);   \
        return;                                                                \
      }                                                                        \
  } while (0)

namespace sonata {

namespace tl = thallium;
using namespace std::string_literals;

class ProviderImpl : public tl::provider<ProviderImpl> {

  auto id() const { return get_provider_id(); } // for convenience

public:
  std::string m_token;
  tl::pool m_pool;
  // Admin RPC
  tl::remote_procedure m_create_database;
  tl::remote_procedure m_attach_database;
  tl::remote_procedure m_detach_database;
  tl::remote_procedure m_destroy_database;
  tl::remote_procedure m_list_databases;
  // Client RPC
  tl::remote_procedure m_exec_on_database;
  tl::remote_procedure m_commit;
  tl::remote_procedure m_open_database;
  tl::remote_procedure m_create_collection;
  tl::remote_procedure m_open_collection;
  tl::remote_procedure m_drop_collection;
  tl::remote_procedure m_coll_store;
  tl::remote_procedure m_coll_store_json;
  tl::remote_procedure m_coll_store_multi;
  tl::remote_procedure m_coll_store_multi_json;
  tl::remote_procedure m_coll_fetch;
  tl::remote_procedure m_coll_fetch_json;
  tl::remote_procedure m_coll_fetch_multi;
  tl::remote_procedure m_coll_fetch_multi_json;
  tl::remote_procedure m_coll_filter;
  tl::remote_procedure m_coll_filter_json;
  tl::remote_procedure m_coll_update;
  tl::remote_procedure m_coll_update_json;
  tl::remote_procedure m_coll_update_multi;
  tl::remote_procedure m_coll_update_multi_json;
  tl::remote_procedure m_coll_all;
  tl::remote_procedure m_coll_all_json;
  tl::remote_procedure m_coll_last_id;
  tl::remote_procedure m_coll_size;
  tl::remote_procedure m_coll_erase;
  tl::remote_procedure m_coll_erase_multi;
  // Backends
  std::unordered_map<std::string, std::shared_ptr<Backend>> m_backends;
  std::unordered_map<std::string, std::string> m_backend_types;
  tl::mutex m_backends_mtx;

  ProviderImpl(tl::engine &engine, uint16_t provider_id, const tl::pool &pool)
      : tl::provider<ProviderImpl>(engine, provider_id), m_pool(pool),
        m_create_database(define("sonata_create_database",
                                 &ProviderImpl::createDatabase, pool)),
        m_attach_database(define("sonata_attach_database",
                                 &ProviderImpl::attachDatabase, pool)),
        m_detach_database(define("sonata_detach_database",
                                 &ProviderImpl::detachDatabase, pool)),
        m_destroy_database(define("sonata_destroy_database",
                                  &ProviderImpl::destroyDatabase, pool)),
        m_list_databases(define("sonata_list_databases",
                                  &ProviderImpl::listDatabases, pool)),
        m_exec_on_database(define("sonata_exec_on_database",
                                  &ProviderImpl::execOnDatabase, pool)),
        m_commit(define("sonata_commit", &ProviderImpl::commit, pool)),
        m_open_database(
            define("sonata_open_database", &ProviderImpl::openDatabase, pool)),
        m_create_collection(define("sonata_create_collection",
                                   &ProviderImpl::createCollection, pool)),
        m_open_collection(define("sonata_open_collection",
                                 &ProviderImpl::openCollection, pool)),
        m_drop_collection(define("sonata_drop_collection",
                                 &ProviderImpl::dropCollection, pool)),
        m_coll_store(define("sonata_store", &ProviderImpl::store, pool)),
        m_coll_store_json(
            define("sonata_store_json", &ProviderImpl::storeJson, pool)),
        m_coll_store_multi(
            define("sonata_store_multi", &ProviderImpl::storeMulti, pool)),
        m_coll_store_multi_json(define("sonata_store_multi_json",
                                       &ProviderImpl::storeMultiJson, pool)),
        m_coll_fetch(define("sonata_fetch", &ProviderImpl::fetch, pool)),
        m_coll_fetch_json(
            define("sonata_fetch_json", &ProviderImpl::fetchJson, pool)),
        m_coll_fetch_multi(
            define("sonata_fetch_multi", &ProviderImpl::fetchMulti, pool)),
        m_coll_fetch_multi_json(define("sonata_fetch_multi_json",
                                       &ProviderImpl::fetchMultiJson, pool)),
        m_coll_filter(define("sonata_filter", &ProviderImpl::filter, pool)),
        m_coll_filter_json(
            define("sonata_filter_json", &ProviderImpl::filterJson, pool)),
        m_coll_update(define("sonata_update", &ProviderImpl::update, pool)),
        m_coll_update_json(
            define("sonata_update_json", &ProviderImpl::updateJson, pool)),
        m_coll_update_multi(
            define("sonata_update_multi", &ProviderImpl::updateMulti, pool)),
        m_coll_update_multi_json(define("sonata_update_multi_json",
                                        &ProviderImpl::updateMultiJson, pool)),
        m_coll_all(define("sonata_all", &ProviderImpl::all, pool)),
        m_coll_all_json(
            define("sonata_all_json", &ProviderImpl::allJson, pool)),
        m_coll_last_id(define("sonata_last_id", &ProviderImpl::lastID, pool)),
        m_coll_size(define("sonata_size", &ProviderImpl::size, pool)),
        m_coll_erase(define("sonata_erase", &ProviderImpl::erase, pool)),
        m_coll_erase_multi(
            define("sonata_erase_multi", &ProviderImpl::eraseMulti, pool)) {
    spdlog::trace("[provider:{0}] Registered provider with id {0}", id());
    if (!m_pool)
      m_pool = engine.get_handler_pool();
  }

  ~ProviderImpl() {
    spdlog::trace("[provider:{}] Deregistering provider", id());
    m_create_database.deregister();
    m_attach_database.deregister();
    m_detach_database.deregister();
    m_destroy_database.deregister();
    m_list_databases.deregister();
    m_exec_on_database.deregister();
    m_open_database.deregister();
    m_commit.deregister();
    m_create_collection.deregister();
    m_open_collection.deregister();
    m_drop_collection.deregister();
    m_coll_store.deregister();
    m_coll_store_json.deregister();
    m_coll_store_multi.deregister();
    m_coll_store_multi_json.deregister();
    m_coll_fetch.deregister();
    m_coll_fetch_json.deregister();
    m_coll_fetch_multi.deregister();
    m_coll_fetch_multi_json.deregister();
    m_coll_filter.deregister();
    m_coll_filter_json.deregister();
    m_coll_update.deregister();
    m_coll_update_json.deregister();
    m_coll_update_multi.deregister();
    m_coll_update_multi_json.deregister();
    m_coll_all.deregister();
    m_coll_all_json.deregister();
    m_coll_last_id.deregister();
    m_coll_size.deregister();
    m_coll_erase.deregister();
    m_coll_erase_multi.deregister();
    spdlog::trace("[provider:{}]    => done!", id());
  }

  void createDatabase(const tl::request &req, const std::string &token,
                      const std::string &db_name, const std::string &db_type,
                      const std::string &db_config) {

    spdlog::trace("[provider:{}] Received createDatabase request", id());
    spdlog::trace("[provider:{}]    => database = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => type = {}", id(), db_type);
    spdlog::trace("[provider:{}]    => config = {}", id(), db_config);

    RequestResult<bool> result;

    if (m_token.size() > 0 && m_token != token) {
      result.success() = false;
      result.error() = "Invalid security token";
      req.respond(result);
      spdlog::error("[provider:{}] Invalid security token {}", id(), token);
      return;
    }

    std::lock_guard<tl::mutex> lock(m_backends_mtx);

    if (m_backends.count(db_name) != 0) {
      result.success() = false;
      result.error() = "Database "s + db_name + " already attached";
      req.respond(result);
      spdlog::error("[provider:{}] Database {} alread attached", id(), db_name);
      return;
    }

    json json_config;
    try {
        json_config = json::parse(db_config);
    } catch(const std::exception& ex) {
      result.success() = false;
      result.error() = ex.what();
      req.respond(result);
      spdlog::error("[provider:{}] Could not parse database configuration for "
                    "database {}",
                    id(), db_name);
      return;
    }

    std::unique_ptr<Backend> backend;
    try {
      backend = BackendFactory::createBackend(db_type, get_engine(), m_pool,
                                              json_config);
    } catch (const std::exception &ex) {
      result.success() = false;
      result.error() = ex.what();
      spdlog::error("[provider:{}] Error when creating database {} of type {}:",
                    id(), db_name, db_type);
      spdlog::error("[provider:{}]    => {}", id(), result.error());
      req.respond(result);
      return;
    }

    if (not backend) {
      result.success() = false;
      result.error() = "Unknown database type "s + db_type;
      spdlog::error("[provider:{}] Unknown database type {} for database {}",
                    id(), db_type, db_name);
      req.respond(result);
      return;
    } else {
      m_backends[db_name] = std::move(backend);
      m_backend_types[db_name] = db_type;
    }

    req.respond(result);
    spdlog::trace("[provider:{}] Successfully created database {} of type {}",
                  id(), db_name, db_type);
  }

  void attachDatabase(const tl::request &req, const std::string &token,
                      const std::string &db_name, const std::string &db_type,
                      const std::string &db_config) {

    spdlog::trace("[provider:{}] Received attachDatabase request", id());
    spdlog::trace("[provider:{}]    => database = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => type = {}", id(), db_type);
    spdlog::trace("[provider:{}]    => config = {}", id(), db_config);

    RequestResult<bool> result;

    if (m_token.size() > 0 && m_token != token) {
      result.success() = false;
      result.error() = "Invalid security token";
      req.respond(result);
      spdlog::error("[provider:{}] Invalid security token {}", id(), token);
      return;
    }

    std::lock_guard<tl::mutex> lock(m_backends_mtx);

    if (m_backends.count(db_name) != 0) {
      result.success() = false;
      result.error() = "Database "s + db_name + " already attached";
      req.respond(result);
      spdlog::error("[provider:{}] Database {} alread attached", id(), db_name);
      return;
    }

    json json_config;
    try {
        json_config = json::parse(db_config);
    } catch(const std::exception& ex) {
      result.success() = false;
      result.error() = ex.what();
      req.respond(result);
      spdlog::error("[provider:{}] Could not parse database configuration for "
                    "database {}",
                    id(), db_name);
      return;
    }

    std::unique_ptr<Backend> backend;
    try {
      backend = BackendFactory::attachBackend(db_type, get_engine(), m_pool,
                                              json_config);
    } catch (const std::exception &ex) {
      result.success() = false;
      result.error() = ex.what();
      spdlog::error(
          "[provider:{}] Error when attaching database {} of type {}:", id(),
          db_name, db_type);
      spdlog::error("[provider:{}]    => {}", id(), result.error());
      req.respond(result);
      return;
    }

    if (not backend) {
      result.success() = false;
      result.error() = "Unknown database type "s + db_type;
      spdlog::error("[provider:{}] Unknown database type {} for database {}",
                    id(), db_type, db_name);
      req.respond(result);
      return;
    } else {
      m_backends[db_name] = std::move(backend);
      m_backend_types[db_name] = db_type;
    }

    req.respond(result);
    spdlog::trace("[provider:{}] Successfully attached database {} of type {}",
                  id(), db_name, db_type);
  }

  void detachDatabase(const tl::request &req, const std::string &token,
                      const std::string &db_name) {
    spdlog::trace(
        "[provider:{}] Received detachDatabase request for database {}", id(),
        db_name);
    RequestResult<bool> result;
    {
      std::lock_guard<tl::mutex> lock(m_backends_mtx);

      if (m_backends.count(db_name) == 0) {
        result.success() = false;
        result.error() = "Database "s + db_name + " not found";
        req.respond(result);
        spdlog::error("[provider:{}] Database {} not found", id(), db_name);
        return;
      }

      m_backends.erase(db_name);
      m_backend_types.erase(db_name);
    }
    req.respond(result);
    spdlog::trace("[provider:{}] Database {} successfully detached", id(),
                  db_name);
  }

  void listDatabases(const tl::request &req, const std::string &token) {
    spdlog::trace(
        "[provider:{}] Received listDatabases request", id());
    RequestResult<std::vector<std::string>> result;
    {
      std::lock_guard<tl::mutex> lock(m_backends_mtx);
      for(auto& backend : m_backends) {
        result.value().push_back(backend.first);
      }
    }
    req.respond(result);
  }

  void destroyDatabase(const tl::request &req, const std::string &token,
                       const std::string &db_name) {
    RequestResult<bool> result;
    spdlog::trace(
        "[provider:{}] Received destroyDatabase request for database {}", id(),
        db_name);

    if (m_token.size() > 0 && m_token != token) {
      result.success() = false;
      result.error() = "Invalid security token";
      req.respond(result);
      spdlog::error("[provider:{}] Invalid security token {}", id(), token);
      return;
    }

    {
      std::lock_guard<tl::mutex> lock(m_backends_mtx);

      if (m_backends.count(db_name) == 0) {
        result.success() = false;
        result.error() = "Database "s + db_name + " not found";
        req.respond(result);
        spdlog::error("[provider:{}] Database {} not found", id(), db_name);
        return;
      }

      result = m_backends[db_name]->destroy();
      m_backends.erase(db_name);
      m_backend_types.erase(db_name);
    }

    req.respond(result);
    spdlog::trace("[provider:{}] Database {} successfully destroyed", id(),
                  db_name);
  }

  void execOnDatabase(const tl::request &req, const std::string &db_name,
                      const std::string &code,
                      const std::unordered_set<std::string> &vars,
                      bool commit) {
    spdlog::trace(
        "provider:{}] Received execOnDatabase request for database {}", id(),
        db_name);
    RequestResult<std::unordered_map<std::string, std::string>> result;
    FIND_DATABASE(db);
    result = db->execute(code, vars, commit);
    req.respond(result);
    spdlog::trace("[provider:{}] Code successfully executed on database {}",
                  id(), db_name);
  }

  void commit(const tl::request &req, const std::string &db_name) {
    spdlog::trace("provider:{}] Received commit request for database {}", id(),
                  db_name);
    RequestResult<bool> result;
    FIND_DATABASE(db);
    result = db->commit();
    req.respond(result);
    spdlog::trace("[provider:{}] Commit successfully executed on database {}",
                  id(), db_name);
  }

  void openDatabase(const tl::request &req, const std::string &db_name) {
    spdlog::trace("[provider:{}] Received openDatabase request for database {}",
                  id(), db_name);
    RequestResult<bool> result;
    FIND_DATABASE(db);
    req.respond(result);
    spdlog::trace("[provider:{}] Database {} successfully opened", id(),
                  db_name);
  }

  void createCollection(const tl::request &req, const std::string &db_name,
                        const std::string &coll_name) {
    spdlog::trace("[provider:{}] Received createCollection request", id());
    spdlog::trace("[provider:{}]    => database = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    RequestResult<bool> result;
    FIND_DATABASE(db);
    result = db->createCollection(coll_name);
    req.respond(result);
    spdlog::trace("[provider:{}] Collection {} successfully created", id(),
                  coll_name);
  }

  void openCollection(const tl::request &req, const std::string &db_name,
                      const std::string &coll_name) {
    spdlog::trace("[provider:{}] Received openCollection request", id());
    spdlog::trace("[provider:{}]    => database = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    RequestResult<bool> result;
    FIND_DATABASE(db);
    result = db->openCollection(coll_name);
    req.respond(result);
    spdlog::trace("[provider:{}] Collection {} successfully opened", id(),
                  coll_name);
  }

  void dropCollection(const tl::request &req, const std::string &db_name,
                      const std::string &coll_name) {
    spdlog::trace("[provider:{}] Received dropCollection request", id());
    spdlog::trace("[provider:{}]    => database = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    RequestResult<bool> result;
    FIND_DATABASE(db);
    result = db->dropCollection(coll_name);
    req.respond(result);
    spdlog::trace("[provider:{}] Collection {} successfully dropped", id(),
                  coll_name);
  }

  void store(const tl::request &req, const std::string &db_name,
             const std::string &coll_name, const std::string &record,
             bool commit) {
    spdlog::trace("[provider:{}] Received store request", id());
    spdlog::trace("[provider:{}]    => database = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    RequestResult<uint64_t> result;
    FIND_DATABASE(db);
    result = db->store(coll_name, record, commit);
    req.respond(result);
    spdlog::trace("[provider:{}] Record successfully stored (id = {})", id(),
                  result.value());
  }

  void storeJson(const tl::request &req, const std::string &db_name,
                 const std::string &coll_name, const JsonWrapper &record,
                 bool commit) {
    spdlog::trace("[provider:{}] Received store request", id());
    spdlog::trace("[provider:{}]    => database = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    RequestResult<uint64_t> result;
    FIND_DATABASE(db);
    result = db->storeJson(coll_name, record, commit);
    req.respond(result);
    spdlog::trace("[provider:{}] Record successfully stored (id = {})", id(),
                  result.value());
  }

  void storeMulti(const tl::request &req, const std::string &db_name,
                  const std::string &coll_name,
                  const std::vector<std::string> &records, bool commit) {
    spdlog::trace("[provider:{}] Received store_multi request", id());
    spdlog::trace("[provider:{}]    => database = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    //spdlog::debug("[provider:{}] Received store_multi request", id());
    RequestResult<std::vector<uint64_t>> result;
    FIND_DATABASE(db);
    result = db->storeMulti(coll_name, records, commit);
    //spdlog::debug("[provider:{}] store_multi executed", id());
    req.respond(result);
    //spdlog::debug("[provider:{}] store_multi response sent", id());
    spdlog::trace("[provider:{}] Record successfully stored", id());
  }

  void storeMultiJson(const tl::request &req, const std::string &db_name,
                      const std::string &coll_name, const JsonWrapper &records,
                      bool commit) {
    spdlog::trace("[provider:{}] Received store_multi request", id());
    spdlog::trace("[provider:{}]    => database = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    RequestResult<std::vector<uint64_t>> result;
    FIND_DATABASE(db);
    result = db->storeMultiJson(coll_name, records, commit);
    req.respond(result);
    spdlog::trace("[provider:{}] Record successfully stored", id());
  }

  void fetch(const tl::request &req, const std::string &db_name,
             const std::string &coll_name, uint64_t record_id) {
    spdlog::trace("[provider:{}] Received fetch request", id());
    spdlog::trace("[provider:{}]    => database   = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    spdlog::trace("[provider:{}]    => record id  = {}", id(), record_id);
    RequestResult<std::string> result;
    FIND_DATABASE(db);
    result = db->fetch(coll_name, record_id);
    req.respond(result);
    spdlog::trace("[provider:{}] Record {} successfully fetched", id(),
                  record_id);
  }

  void fetchJson(const tl::request &req, const std::string &db_name,
                 const std::string &coll_name, uint64_t record_id) {
    spdlog::trace("[provider:{}] Received fetch request", id());
    spdlog::trace("[provider:{}]    => database   = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    spdlog::trace("[provider:{}]    => record id  = {}", id(), record_id);
    RequestResult<JsonWrapper> result;
    FIND_DATABASE(db);
    result = db->fetchJson(coll_name, record_id);
    req.respond(result);
    spdlog::trace("[provider:{}] Record {} successfully fetched", id(),
                  record_id);
  }

  void fetchMulti(const tl::request &req, const std::string &db_name,
                  const std::string &coll_name,
                  const std::vector<uint64_t> &record_ids) {
    spdlog::trace("[provider:{}] Received fetch_multi request", id());
    spdlog::trace("[provider:{}]    => database   = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    RequestResult<std::vector<std::string>> result;
    FIND_DATABASE(db);
    result = db->fetchMulti(coll_name, record_ids);
    req.respond(result);
    spdlog::trace("[provider:{}] Records successfully fetched", id());
  }

  void fetchMultiJson(const tl::request &req, const std::string &db_name,
                      const std::string &coll_name,
                      const std::vector<uint64_t> &record_ids) {
    spdlog::trace("[provider:{}] Received fetch_multi request", id());
    spdlog::trace("[provider:{}]    => database   = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    RequestResult<JsonWrapper> result;
    FIND_DATABASE(db);
    result = db->fetchMultiJson(coll_name, record_ids);
    req.respond(result);
    spdlog::trace("[provider:{}] Records successfully fetched", id());
  }

  void filter(const tl::request &req, const std::string &db_name,
              const std::string &coll_name, const std::string &filter_code) {
    spdlog::trace("[provider:{}] Received filter request", id());
    spdlog::trace("[provider:{}]    => database = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    RequestResult<std::vector<std::string>> result;
    FIND_DATABASE(db);
    result = db->filter(coll_name, filter_code);
    req.respond(result);
    spdlog::trace("[provider:{}] Filter successfully executed", id());
  }

  void filterJson(const tl::request &req, const std::string &db_name,
                  const std::string &coll_name,
                  const std::string &filter_code) {
    spdlog::trace("[provider:{}] Received filter request", id());
    spdlog::trace("[provider:{}]    => database = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    RequestResult<JsonWrapper> result;
    FIND_DATABASE(db);
    result = db->filterJson(coll_name, filter_code);
    req.respond(result);
    spdlog::trace("[provider:{}] Filter successfully executed", id());
  }

  void update(const tl::request &req, const std::string &db_name,
              const std::string &coll_name, uint64_t record_id,
              const std::string &new_content, bool commit) {
    spdlog::trace("[provider:{}] Received update request", id());
    spdlog::trace("[provider:{}]    => database = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    spdlog::trace("[provider:{}]    => record id = {}", id(), record_id);
    RequestResult<bool> result;
    FIND_DATABASE(db);
    result = db->update(coll_name, record_id, new_content, commit);
    req.respond(result);
    spdlog::trace("[provider:{}] Update successfully applied to record {}",
                  id(), record_id);
  }

  void updateJson(const tl::request &req, const std::string &db_name,
                  const std::string &coll_name, uint64_t record_id,
                  const JsonWrapper &new_content, bool commit) {
    spdlog::trace("[provider:{}] Received update request", id());
    spdlog::trace("[provider:{}]    => database = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    spdlog::trace("[provider:{}]    => record id = {}", id(), record_id);
    RequestResult<bool> result;
    FIND_DATABASE(db);
    result = db->updateJson(coll_name, record_id, new_content, commit);
    req.respond(result);
    spdlog::trace("[provider:{}] Update successfully applied to record {}",
                  id(), record_id);
  }

  void updateMulti(const tl::request &req, const std::string &db_name,
                   const std::string &coll_name,
                   const std::vector<uint64_t> &record_ids,
                   const std::vector<std::string> &new_contents, bool commit) {
    spdlog::trace("[provider:{}] Received update request", id());
    spdlog::trace("[provider:{}]    => database = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    RequestResult<std::vector<bool>> result;
    FIND_DATABASE(db);
    result = db->updateMulti(coll_name, record_ids, new_contents, commit);
    req.respond(result);
    spdlog::trace("[provider:{}] Update successfully applied to records", id());
  }

  void updateMultiJson(const tl::request &req, const std::string &db_name,
                       const std::string &coll_name,
                       const std::vector<uint64_t> &record_ids,
                       const JsonWrapper &new_content, bool commit) {
    spdlog::trace("[provider:{}] Received update request", id());
    spdlog::trace("[provider:{}]    => database = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    RequestResult<std::vector<bool>> result;
    FIND_DATABASE(db);
    result = db->updateMultiJson(coll_name, record_ids, new_content, commit);
    req.respond(result);
    spdlog::trace("[provider:{}] Update successfully applied to records", id());
  }

  void all(const tl::request &req, const std::string &db_name,
           const std::string &coll_name) {
    spdlog::trace("[provider:{}] Received all request", id());
    spdlog::trace("[provider:{}]    => database = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    RequestResult<std::vector<std::string>> result;
    FIND_DATABASE(db);
    result = db->all(coll_name);
    req.respond(result);
    spdlog::trace("[provider:{}] Successfully returned the full collection {}",
                  id(), coll_name);
  }

  void allJson(const tl::request &req, const std::string &db_name,
               const std::string &coll_name) {
    spdlog::trace("[provider:{}] Received all request", id());
    spdlog::trace("[provider:{}]    => database = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    RequestResult<JsonWrapper> result;
    FIND_DATABASE(db);
    result = db->allJson(coll_name);
    req.respond(result);
    spdlog::trace("[provider:{}] Successfully returned the full collection {}",
                  id(), coll_name);
  }

  void lastID(const tl::request &req, const std::string &db_name,
              const std::string &coll_name) {
    spdlog::trace("[provider:{}] Received lastID request", id());
    spdlog::trace("[provider:{}]    => database = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    RequestResult<uint64_t> result;
    FIND_DATABASE(db);
    result = db->lastID(coll_name);
    req.respond(result);
    spdlog::trace("[provider:{}] Successfully returned the last id ({})", id(),
                  result.value());
  }

  void size(const tl::request &req, const std::string &db_name,
            const std::string &coll_name) {
    spdlog::trace("[provider:{}] Received size request", id());
    spdlog::trace("[provider:{}]    => database = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    RequestResult<size_t> result;
    FIND_DATABASE(db);
    result = db->size(coll_name);
    req.respond(result);
    spdlog::trace("[provider:{}] Successfully returned collection size ({})",
                  id(), result.value());
  }

  void erase(const tl::request &req, const std::string &db_name,
             const std::string &coll_name, uint64_t record_id, bool commit) {
    spdlog::trace("[provider:{}] Received erase request", id());
    spdlog::trace("[provider:{}]    => database = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    spdlog::trace("[provider:{}]    => record id = {}", id(), record_id);
    RequestResult<bool> result;
    FIND_DATABASE(db);
    result = db->erase(coll_name, record_id, commit);
    req.respond(result);
    spdlog::trace("[provider:{}] Successfully erased record {}", id(),
                  record_id);
  }

  void eraseMulti(const tl::request &req, const std::string &db_name,
                  const std::string &coll_name,
                  const std::vector<uint64_t> &record_ids, bool commit) {
    spdlog::trace("[provider:{}] Received erase request", id());
    spdlog::trace("[provider:{}]    => database = {}", id(), db_name);
    spdlog::trace("[provider:{}]    => collection = {}", id(), coll_name);
    RequestResult<bool> result;
    FIND_DATABASE(db);
    result = db->eraseMulti(coll_name, record_ids, commit);
    req.respond(result);
    spdlog::trace("[provider:{}] Successfully erased records", id());
  }
};

} // namespace sonata

#endif
