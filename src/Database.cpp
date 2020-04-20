/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "sonata/Database.hpp"
#include "sonata/Collection.hpp"
#include "sonata/RequestResult.hpp"

#include "ClientImpl.hpp"
#include "DatabaseImpl.hpp"
#include "CollectionImpl.hpp"

#include <thallium/serialization/stl/string.hpp>

namespace sonata {

Database::Database() = default;

Database::Database(const std::shared_ptr<DatabaseImpl>& impl)
: self(impl) {}

Database::Database(Database&& other) = default;

Database& Database::operator=(Database&& other) = default;

Database::Database(const Database& other) = default;

Database& Database::operator=(const Database& other) = default;

Database::~Database() = default;

Database::operator bool() const {
    return static_cast<bool>(self);
}

Client Database::client() const {
    return Client(self->m_client);
}

Collection Database::create(const std::string& collectionName) const {
    if(not self) throw Exception("Invalid sonata::Database object");
    RequestResult<bool> result = self->m_client->m_create_collection.on(self->m_ph)(self->m_name, collectionName);
    if(result.success()) {
        auto coll_impl = std::make_shared<CollectionImpl>(self, collectionName);
        return Collection(coll_impl);
    } else {
        throw Exception(result.error());
        return Collection(nullptr);
    }
}

Collection Database::open(const std::string& collectionName) const {
    if(not self) throw Exception("Invalid sonata::Database object");
    RequestResult<bool> result = self->m_client->m_open_collection.on(self->m_ph)(self->m_name, collectionName);
    if(result.success()) {
        auto coll_impl = std::make_shared<CollectionImpl>(self, collectionName);
        return Collection(coll_impl);
    } else {
        throw Exception(result.error());
        return Collection(nullptr);
    }
}

void Database::drop(const std::string& collectionName) const {
    if(not self) throw Exception("Invalid sonata::Database object");
    RequestResult<bool> result = self->m_client->m_drop_collection.on(self->m_ph)(self->m_name, collectionName);
    if(not result.success()) {
        throw Exception(result.error());
    }
}

bool Database::exists(const std::string& collectionName) const {
    if(not self) throw Exception("Invalid sonata::Database object");
    RequestResult<bool> result = self->m_client->m_open_collection.on(self->m_ph)(self->m_name, collectionName);
    return result.success();
}

void Database::execute(const std::string& code,
                       const std::unordered_set<std::string>& vars,
                       std::unordered_map<std::string,std::string>* out) const {
    if(not self) throw Exception("Invalid sonata::Database object");
    RequestResult<std::unordered_map<std::string,std::string>> result
        = self->m_client->m_execute_on_database.on(self->m_ph)(self->m_name, code, vars);
    if(not result.success()) {
        throw Exception(result.error());
    }
    if(out)
        *out = std::move(result.value());
}

void Database::execute(const std::string& code,
                       const std::unordered_set<std::string>& vars,
                       Json::Value* result) const {
    if(not self) throw Exception("Invalid sonata::Database object");
    std::unordered_map<std::string,std::string> ret;
    if(result) {
        std::unordered_map<std::string,std::string> ret;
        execute(code, vars, &ret);
        Json::Value tmp_result;
        for(auto& p : ret) {
            Json::Value tmp;
            const auto& name  = p.first;
            const auto& value = p.second;
            std::string errors;
            bool parsingSuccessful = self->m_json_reader->parse(value.c_str(),
                                               value.c_str() + value.size(),
                                               &tmp,
                                               &errors);
            if(!parsingSuccessful) {
                throw Exception(errors);
            }
            tmp_result[name] = std::move(tmp);
        }
        *result = std::move(tmp_result);
    } else {
        execute(code, vars, static_cast<std::unordered_map<std::string,std::string>*>(nullptr));
    }
}

}
