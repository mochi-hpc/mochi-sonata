#include "sonata/Database.hpp"
#include "sonata/Collection.hpp"
#include "sonata/RequestResult.hpp"

#include "ClientImpl.hpp"
#include "DatabaseImpl.hpp"
#include "CollectionImpl.hpp"

#include <thallium/serialization/stl/string.hpp>

namespace sonata {

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

Collection Database::create(const std::string& collectionName) const {
    if(not self) throw std::runtime_error("Invalid sonata::Database object");
    RequestResult<bool> result = self->m_client->m_create_collection.on(self->m_ph)(self->m_name, collectionName);
    if(result.success()) {
        auto coll_impl = std::make_shared<CollectionImpl>(self, collectionName);
        return Collection(coll_impl);
    } else {
        throw std::runtime_error(result.error());
        return Collection(nullptr);
    }
}

Collection Database::open(const std::string& collectionName) const {
    if(not self) throw std::runtime_error("Invalid sonata::Database object");
    RequestResult<bool> result = self->m_client->m_open_collection.on(self->m_ph)(self->m_name, collectionName);
    if(result.success()) {
        auto coll_impl = std::make_shared<CollectionImpl>(self, collectionName);
        return Collection(coll_impl);
    } else {
        throw std::runtime_error(result.error());
        return Collection(nullptr);
    }
}

void Database::drop(const std::string& collectionName) const {
    if(not self) throw std::runtime_error("Invalid sonata::Database object");
    RequestResult<bool> result = self->m_client->m_drop_collection.on(self->m_ph)(self->m_name, collectionName);
    if(not result.success()) {
        throw std::runtime_error(result.error());
    }
}

bool Database::exists(const std::string& collectionName) const {
    if(not self) throw std::runtime_error("Invalid sonata::Database object");
    RequestResult<bool> result = self->m_client->m_open_collection.on(self->m_ph)(self->m_name, collectionName);
    return result.success();
}

}
