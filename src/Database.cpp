/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "sonata/Database.hpp"
#include "sonata/Collection.hpp"
#include "sonata/RequestResult.hpp"

#include "ClientImpl.hpp"
#include "CollectionImpl.hpp"
#include "DatabaseImpl.hpp"

#include <thallium/serialization/stl/string.hpp>

namespace sonata {

Database::Database() = default;

Database::Database(const std::shared_ptr<DatabaseImpl> &impl) : self(impl) {}

Database::Database(Database &&other) = default;

Database &Database::operator=(Database &&other) = default;

Database::Database(const Database &other) = default;

Database &Database::operator=(const Database &other) = default;

Database::~Database() = default;

Database::operator bool() const { return static_cast<bool>(self); }

Client Database::client() const { return Client(self->m_client); }

Collection Database::create(const std::string &collectionName) const {
  if (not self)
    throw Exception("Invalid sonata::Database object");
  RequestResult<bool> result = self->m_client->m_create_collection.on(
      self->m_ph)(self->m_name, collectionName);
  if (result.success()) {
    auto coll_impl = std::make_shared<CollectionImpl>(self, collectionName);
    return Collection(coll_impl);
  } else {
    throw Exception(result.error());
    return Collection(nullptr);
  }
}

Collection Database::open(const std::string &collectionName, bool check) const {
  if (not self)
    throw Exception("Invalid sonata::Database object");
  RequestResult<bool> result;
  result.success() = true;
  if (check)
    result = self->m_client->m_open_collection.on(self->m_ph)(self->m_name,
                                                              collectionName);
  if (result.success()) {
    auto coll_impl = std::make_shared<CollectionImpl>(self, collectionName);
    return Collection(coll_impl);
  } else {
    throw Exception(result.error());
    return Collection(nullptr);
  }
}

void Database::drop(const std::string &collectionName) const {
  if (not self)
    throw Exception("Invalid sonata::Database object");
  RequestResult<bool> result = self->m_client->m_drop_collection.on(self->m_ph)(
      self->m_name, collectionName);
  if (not result.success()) {
    throw Exception(result.error());
  }
}

bool Database::exists(const std::string &collectionName) const {
  if (not self)
    throw Exception("Invalid sonata::Database object");
  RequestResult<bool> result = self->m_client->m_open_collection.on(self->m_ph)(
      self->m_name, collectionName);
  return result.success();
}

void Database::execute(const std::string &code,
                       const std::unordered_set<std::string> &vars,
                       std::unordered_map<std::string, std::string> *out,
                       bool commit) const {
  if (not self)
    throw Exception("Invalid sonata::Database object");
  RequestResult<std::unordered_map<std::string, std::string>> result =
      self->m_client->m_execute_on_database.on(self->m_ph)(self->m_name, code,
                                                           vars, commit);
  if (not result.success()) {
    throw Exception(result.error());
  }
  if (out)
    *out = std::move(result.value());
}

void Database::execute(const std::string &code,
                       const std::unordered_set<std::string> &vars,
                       json *result, bool commit) const {
  if (not self)
    throw Exception("Invalid sonata::Database object");
  std::unordered_map<std::string, std::string> ret;
  if (result) {
    std::unordered_map<std::string, std::string> ret;
    execute(code, vars, &ret, commit);
    json tmp = json::object();
    for (auto &p : ret) {
      const auto &name = p.first;
      const auto &value = p.second;
      auto val = json::parse(value);
      tmp[name] = std::move(val);
    }
    *result = std::move(tmp);
  } else {
    execute(
        code, vars,
        static_cast<std::unordered_map<std::string, std::string> *>(nullptr));
  }
}

void Database::commit() const {
  if (not self)
    throw Exception("Invalid sonata::Database object");
  RequestResult<bool> result =
      self->m_client->m_commit.on(self->m_ph)(self->m_name);
  if (not result.success()) {
    throw Exception(result.error());
  }
}

} // namespace sonata
