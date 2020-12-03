/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "sonata/Collection.hpp"
#include "sonata/Exception.hpp"
#include "sonata/JsonSerialize.hpp"
#include "sonata/RequestResult.hpp"

#include "AsyncRequestImpl.hpp"
#include "ClientImpl.hpp"
#include "CollectionImpl.hpp"
#include "DatabaseImpl.hpp"

#include <thallium/serialization/stl/pair.hpp>
#include <thallium/serialization/stl/string.hpp>

namespace sonata {

Collection::Collection() = default;

Collection::Collection(const std::shared_ptr<CollectionImpl> &impl)
    : self(impl) {}

Collection::Collection(const Collection &) = default;

Collection::Collection(Collection &&) = default;

Collection &Collection::operator=(const Collection &) = default;

Collection &Collection::operator=(Collection &&) = default;

Collection::~Collection() = default;

Collection::operator bool() const { return static_cast<bool>(self); }

Database Collection::database() const { return Database(self->m_database); }

void Collection::store(const Json::Value &record, uint64_t *id, bool commit,
                       AsyncRequest *req) const {
  if (not self)
    throw Exception("Invalid sonata::Collection object");
  auto &rpc = self->m_database->m_client->m_coll_store_json;
  auto &ph = self->m_database->m_ph;
  auto &db_name = self->m_database->m_name;
  auto async_response = rpc.on(ph).async(db_name, self->m_name, record, commit);
  auto async_request_impl =
      std::make_shared<AsyncRequestImpl>(std::move(async_response));
  async_request_impl->m_wait_callback =
      [id](AsyncRequestImpl &async_request_impl) {
        RequestResult<uint64_t> result =
            async_request_impl.m_async_response.wait();
        if (result.success()) {
          if (id)
            *id = result.value();
        } else {
          throw Exception(result.error());
        }
      };
  if (req)
    *req = AsyncRequest(std::move(async_request_impl));
  else
    AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::store(const std::string &record, uint64_t *id, bool commit,
                       AsyncRequest *req) const {
  if (not self)
    throw Exception("Invalid sonata::Collection object");
  auto &rpc = self->m_database->m_client->m_coll_store;
  auto &ph = self->m_database->m_ph;
  auto &db_name = self->m_database->m_name;
  auto async_response = rpc.on(ph).async(db_name, self->m_name, record, commit);
  auto async_request_impl =
      std::make_shared<AsyncRequestImpl>(std::move(async_response));
  async_request_impl->m_wait_callback =
      [id](AsyncRequestImpl &async_request_impl) {
        RequestResult<uint64_t> result =
            async_request_impl.m_async_response.wait();
        if (result.success()) {
          if (id)
            *id = result.value();
        } else {
          throw Exception(result.error());
        }
      };
  if (req)
    *req = AsyncRequest(std::move(async_request_impl));
  else
    AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::store_multi(const Json::Value &records, uint64_t *ids,
                             bool commit, AsyncRequest *req) const {
  if (not self)
    throw Exception("Invalid sonata::Collection object");
  if (records.type() != Json::arrayValue) {
    throw Exception("JSON object is not of Array type");
  }
  auto &rpc = self->m_database->m_client->m_coll_store_multi_json;
  auto &ph = self->m_database->m_ph;
  auto &db_name = self->m_database->m_name;
  auto async_response =
      rpc.on(ph).async(db_name, self->m_name, records, commit);
  auto async_request_impl =
      std::make_shared<AsyncRequestImpl>(std::move(async_response));
  async_request_impl->m_wait_callback =
      [ids](AsyncRequestImpl &async_request_impl) {
        RequestResult<std::vector<uint64_t>> result =
            async_request_impl.m_async_response.wait();
        if (result.success()) {
          if (ids)
            memcpy(ids, result.value().data(),
                   result.value().size() * sizeof(*ids));
        } else {
          throw Exception(result.error());
        }
      };
  if (req)
    *req = AsyncRequest(std::move(async_request_impl));
  else
    AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::store_multi(const std::vector<std::string> &records,
                             uint64_t *ids, bool commit,
                             AsyncRequest *req) const {
  if (not self)
    throw Exception("Invalid sonata::Collection object");
  auto &rpc = self->m_database->m_client->m_coll_store_multi;
  auto &ph = self->m_database->m_ph;
  auto &db_name = self->m_database->m_name;
  auto async_response =
      rpc.on(ph).async(db_name, self->m_name, records, commit);
  auto async_request_impl =
      std::make_shared<AsyncRequestImpl>(std::move(async_response));
  async_request_impl->m_wait_callback =
      [ids](AsyncRequestImpl &async_request_impl) {
        RequestResult<std::vector<uint64_t>> result =
            async_request_impl.m_async_response.wait();
        if (result.success()) {
          if (ids)
            memcpy(ids, result.value().data(),
                   result.value().size() * sizeof(*ids));
        } else {
          throw Exception(result.error());
        }
      };
  if (req)
    *req = AsyncRequest(std::move(async_request_impl));
  else
    AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::fetch(uint64_t id, std::string *out, AsyncRequest *req) const {
  if (not out)
    return;
  if (not self)
    throw Exception("Invalid sonata::Collection object");
  auto &rpc = self->m_database->m_client->m_coll_fetch;
  auto &ph = self->m_database->m_ph;
  auto &db_name = self->m_database->m_name;
  auto async_response = rpc.on(ph).async(db_name, self->m_name, id);
  auto async_request_impl =
      std::make_shared<AsyncRequestImpl>(std::move(async_response));
  async_request_impl->m_wait_callback =
      [out](AsyncRequestImpl &async_request_impl) {
        RequestResult<std::string> result =
            async_request_impl.m_async_response.wait();
        if (result.success()) {
          *out = std::move(result.value());
        } else {
          throw Exception(result.error());
        }
      };
  if (req)
    *req = AsyncRequest(std::move(async_request_impl));
  else
    AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::fetch(uint64_t id, Json::Value *out, AsyncRequest *req) const {
  if (not out)
    return;
  if (not self)
    throw Exception("Invalid sonata::Collection object");
  auto &rpc = self->m_database->m_client->m_coll_fetch_json;
  auto &ph = self->m_database->m_ph;
  auto &db_name = self->m_database->m_name;
  auto async_response = rpc.on(ph).async(db_name, self->m_name, id);
  auto async_request_impl =
      std::make_shared<AsyncRequestImpl>(std::move(async_response));
  async_request_impl->m_wait_callback =
      [out, self = self](AsyncRequestImpl &async_request_impl) {
        RequestResult<Json::Value> result =
            async_request_impl.m_async_response.wait();
        if (result.success()) {
          *out = std::move(result.value());
        } else {
          throw Exception(result.error());
        }
      };
  if (req)
    *req = AsyncRequest(std::move(async_request_impl));
  else
    AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::fetch_multi(const uint64_t *ids, size_t count,
                             std::vector<std::string> *out,
                             AsyncRequest *req) const {
  if (not out)
    return;
  if (not self)
    throw Exception("Invalid sonata::Collection object");
  auto &rpc = self->m_database->m_client->m_coll_fetch_multi;
  auto &ph = self->m_database->m_ph;
  auto &db_name = self->m_database->m_name;
  std::vector<uint64_t> ids_vec(ids, ids + count);
  auto async_response = rpc.on(ph).async(db_name, self->m_name, ids_vec);
  auto async_request_impl =
      std::make_shared<AsyncRequestImpl>(std::move(async_response));
  async_request_impl->m_wait_callback =
      [out](AsyncRequestImpl &async_request_impl) {
        RequestResult<std::vector<std::string>> result =
            async_request_impl.m_async_response.wait();
        if (result.success()) {
          *out = std::move(result.value());
        } else {
          throw Exception(result.error());
        }
      };
  if (req)
    *req = AsyncRequest(std::move(async_request_impl));
  else
    AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::fetch_multi(const uint64_t *ids, size_t count,
                             Json::Value *out, AsyncRequest *req) const {
  if (not out)
    return;
  if (not self)
    throw Exception("Invalid sonata::Collection object");
  auto &rpc = self->m_database->m_client->m_coll_fetch_multi_json;
  auto &ph = self->m_database->m_ph;
  auto &db_name = self->m_database->m_name;
  std::vector<uint64_t> ids_vec(ids, ids + count);
  auto async_response = rpc.on(ph).async(db_name, self->m_name, ids_vec);
  auto async_request_impl =
      std::make_shared<AsyncRequestImpl>(std::move(async_response));
  async_request_impl->m_wait_callback =
      [out, self = self](AsyncRequestImpl &async_request_impl) {
        RequestResult<Json::Value> result =
            async_request_impl.m_async_response.wait();
        if (result.success()) {
          *out = std::move(result.value());
        } else {
          throw Exception(result.error());
        }
      };
  if (req)
    *req = AsyncRequest(std::move(async_request_impl));
  else
    AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::filter(const std::string &filterCode,
                        std::vector<std::string> *out,
                        AsyncRequest *req) const {
  if (not self)
    throw Exception("Invalid sonata::Collection object");
  auto &rpc = self->m_database->m_client->m_coll_filter;
  auto &ph = self->m_database->m_ph;
  auto &db_name = self->m_database->m_name;
  auto async_response = rpc.on(ph).async(db_name, self->m_name, filterCode);
  auto async_request_impl =
      std::make_shared<AsyncRequestImpl>(std::move(async_response));
  async_request_impl->m_wait_callback =
      [out](AsyncRequestImpl &async_request_impl) {
        RequestResult<std::vector<std::string>> result =
            async_request_impl.m_async_response.wait();
        if (result.success()) {
          if (out)
            *out = std::move(result.value());
        } else {
          throw Exception(result.error());
        }
      };
  if (req)
    *req = AsyncRequest(std::move(async_request_impl));
  else
    AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::filter(const std::string &filterCode, Json::Value *out,
                        AsyncRequest *req) const {
  if (not self)
    throw Exception("Invalid sonata::Collection object");
  auto &rpc = self->m_database->m_client->m_coll_filter_json;
  auto &ph = self->m_database->m_ph;
  auto &db_name = self->m_database->m_name;
  auto async_response = rpc.on(ph).async(db_name, self->m_name, filterCode);
  auto async_request_impl =
      std::make_shared<AsyncRequestImpl>(std::move(async_response));
  async_request_impl->m_wait_callback =
      [out, self = self](AsyncRequestImpl &async_request_impl) {
        RequestResult<Json::Value> result =
            async_request_impl.m_async_response.wait();
        if (result.success()) {
          if (out)
            *out = std::move(result.value());
        } else {
          throw Exception(result.error());
        }
      };
  if (req)
    *req = AsyncRequest(std::move(async_request_impl));
  else
    AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::update(uint64_t id, const std::string &record, bool commit,
                        AsyncRequest *req) const {
  if (not self)
    throw Exception("Invalid sonata::Collection object");
  auto &rpc = self->m_database->m_client->m_coll_update;
  auto &ph = self->m_database->m_ph;
  auto &db_name = self->m_database->m_name;
  auto async_response =
      rpc.on(ph).async(db_name, self->m_name, id, record, commit);
  auto async_request_impl =
      std::make_shared<AsyncRequestImpl>(std::move(async_response));
  async_request_impl->m_wait_callback =
      [](AsyncRequestImpl &async_request_impl) {
        RequestResult<bool> result = async_request_impl.m_async_response.wait();
        if (!result.success()) {
          throw Exception(result.error());
        }
      };
  if (req)
    *req = AsyncRequest(std::move(async_request_impl));
  else
    AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::update(uint64_t id, const Json::Value &record, bool commit,
                        AsyncRequest *req) const {
  if (not self)
    throw Exception("Invalid sonata::Collection object");
  auto &rpc = self->m_database->m_client->m_coll_update_json;
  auto &ph = self->m_database->m_ph;
  auto &db_name = self->m_database->m_name;
  auto async_response =
      rpc.on(ph).async(db_name, self->m_name, id, record, commit);
  auto async_request_impl =
      std::make_shared<AsyncRequestImpl>(std::move(async_response));
  async_request_impl->m_wait_callback =
      [](AsyncRequestImpl &async_request_impl) {
        RequestResult<bool> result = async_request_impl.m_async_response.wait();
        if (!result.success()) {
          throw Exception(result.error());
        }
      };
  if (req)
    *req = AsyncRequest(std::move(async_request_impl));
  else
    AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::update_multi(const uint64_t *ids,
                              const std::vector<std::string> &records,
                              std::vector<bool> *updated, bool commit,
                              AsyncRequest *req) const {
  if (not self)
    throw Exception("Invalid sonata::Collection object");
  auto &rpc = self->m_database->m_client->m_coll_update_multi;
  auto &ph = self->m_database->m_ph;
  auto &db_name = self->m_database->m_name;
  std::vector<uint64_t> ids_vec(ids, ids + records.size());
  auto async_response =
      rpc.on(ph).async(db_name, self->m_name, ids_vec, records, commit);
  auto async_request_impl =
      std::make_shared<AsyncRequestImpl>(std::move(async_response));
  async_request_impl->m_wait_callback =
      [updated](AsyncRequestImpl &async_request_impl) {
        RequestResult<std::vector<bool>> result =
            async_request_impl.m_async_response.wait();
        if (!result.success()) {
          throw Exception(result.error());
        } else {
          if (updated != nullptr) {
            *updated = result.value();
          }
        }
      };
  if (req)
    *req = AsyncRequest(std::move(async_request_impl));
  else
    AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::update_multi(const uint64_t *ids, const Json::Value &records,
                              std::vector<bool> *updated, bool commit,
                              AsyncRequest *req) const {
  if (not self)
    throw Exception("Invalid sonata::Collection object");
  if (records.type() != Json::arrayValue) {
    throw Exception("JSON object is not of Array type");
  }
  auto &rpc = self->m_database->m_client->m_coll_update_multi_json;
  auto &ph = self->m_database->m_ph;
  auto &db_name = self->m_database->m_name;
  std::vector<uint64_t> ids_vec(ids, ids + records.size());
  auto async_response =
      rpc.on(ph).async(db_name, self->m_name, ids_vec, records, commit);
  auto async_request_impl =
      std::make_shared<AsyncRequestImpl>(std::move(async_response));
  async_request_impl->m_wait_callback =
      [updated](AsyncRequestImpl &async_request_impl) {
        RequestResult<std::vector<bool>> result =
            async_request_impl.m_async_response.wait();
        if (!result.success()) {
          throw Exception(result.error());
        } else {
          if (updated != nullptr) {
            *updated = result.value();
          }
        }
      };
  if (req)
    *req = AsyncRequest(std::move(async_request_impl));
  else
    AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::all(std::vector<std::string> *out, AsyncRequest *req) const {
  if (not out)
    return;
  if (not self)
    throw Exception("Invalid sonata::Collection object");
  auto &rpc = self->m_database->m_client->m_coll_all;
  auto &ph = self->m_database->m_ph;
  auto &db_name = self->m_database->m_name;
  auto async_response = rpc.on(ph).async(db_name, self->m_name);
  auto async_request_impl =
      std::make_shared<AsyncRequestImpl>(std::move(async_response));
  async_request_impl->m_wait_callback =
      [out](AsyncRequestImpl &async_request_impl) {
        RequestResult<std::vector<std::string>> result =
            async_request_impl.m_async_response.wait();
        if (result.success()) {
          *out = std::move(result.value());
        } else {
          throw Exception(result.error());
        }
      };
  if (req)
    *req = AsyncRequest(std::move(async_request_impl));
  else
    AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::all(Json::Value *out, AsyncRequest *req) const {
  if (not self)
    throw Exception("Invalid sonata::Collection object");
  auto &rpc = self->m_database->m_client->m_coll_all_json;
  auto &ph = self->m_database->m_ph;
  auto &db_name = self->m_database->m_name;
  auto async_response = rpc.on(ph).async(db_name, self->m_name);
  auto async_request_impl =
      std::make_shared<AsyncRequestImpl>(std::move(async_response));
  async_request_impl->m_wait_callback =
      [out, self = self](AsyncRequestImpl &async_request_impl) {
        RequestResult<Json::Value> result =
            async_request_impl.m_async_response.wait();
        if (result.success()) {
          if (out)
            *out = std::move(result.value());
        } else {
          throw Exception(result.error());
        }
      };
  if (req)
    *req = AsyncRequest(std::move(async_request_impl));
  else
    AsyncRequest(std::move(async_request_impl)).wait();
}

uint64_t Collection::last_record_id() const {
  if (not self)
    throw Exception("Invalid sonata::Collection object");
  auto &rpc = self->m_database->m_client->m_coll_last_id;
  auto &ph = self->m_database->m_ph;
  auto &db_name = self->m_database->m_name;
  RequestResult<uint64_t> result = rpc.on(ph)(db_name, self->m_name);
  if (not result.success())
    throw Exception(result.error());
  return result.value();
}

size_t Collection::size() const {
  if (not self)
    throw Exception("Invalid sonata::Collection object");
  auto &rpc = self->m_database->m_client->m_coll_size;
  auto &ph = self->m_database->m_ph;
  auto &db_name = self->m_database->m_name;
  RequestResult<size_t> result = rpc.on(ph)(db_name, self->m_name);
  if (not result.success())
    throw Exception(result.error());
  return result.value();
}

void Collection::erase(uint64_t id, bool commit, AsyncRequest *req) const {
  if (not self)
    throw Exception("Invalid sonata::Collection object");
  auto &rpc = self->m_database->m_client->m_coll_erase;
  auto &ph = self->m_database->m_ph;
  auto &db_name = self->m_database->m_name;
  auto async_response = rpc.on(ph).async(db_name, self->m_name, id, commit);
  auto async_request_impl =
      std::make_shared<AsyncRequestImpl>(std::move(async_response));
  async_request_impl->m_wait_callback =
      [](AsyncRequestImpl &async_request_impl) {
        RequestResult<bool> result = async_request_impl.m_async_response.wait();
        if (!result.success()) {
          throw Exception(result.error());
        }
      };
  if (req)
    *req = AsyncRequest(std::move(async_request_impl));
  else
    AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::erase_multi(const uint64_t *ids, size_t count, bool commit,
                             AsyncRequest *req) const {
  if (not self)
    throw Exception("Invalid sonata::Collection object");
  auto &rpc = self->m_database->m_client->m_coll_erase_multi;
  auto &ph = self->m_database->m_ph;
  auto &db_name = self->m_database->m_name;
  std::vector<uint64_t> ids_vec(ids, ids + count);
  auto async_response =
      rpc.on(ph).async(db_name, self->m_name, ids_vec, commit);
  auto async_request_impl =
      std::make_shared<AsyncRequestImpl>(std::move(async_response));
  async_request_impl->m_wait_callback =
      [](AsyncRequestImpl &async_request_impl) {
        RequestResult<bool> result = async_request_impl.m_async_response.wait();
        if (!result.success()) {
          throw Exception(result.error());
        }
      };
  if (req)
    *req = AsyncRequest(std::move(async_request_impl));
  else
    AsyncRequest(std::move(async_request_impl)).wait();
}

} // namespace sonata
