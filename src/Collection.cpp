#include "sonata/Collection.hpp"
#include "sonata/RequestResult.hpp"

#include "ClientImpl.hpp"
#include "DatabaseImpl.hpp"
#include "CollectionImpl.hpp"

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace sonata {

Collection::Collection(const std::shared_ptr<CollectionImpl>& impl)
: self(impl) {}

Collection::Collection(const Collection&) = default;

Collection::Collection(Collection&&) = default;

Collection& Collection::operator=(const Collection&) = default;

Collection& Collection::operator=(Collection&&) = default;

Collection::~Collection() = default;

Collection::operator bool() const {
    return static_cast<bool>(self);
}

uint64_t Collection::store(const std::string& record) const {
    if(not self) throw std::runtime_error("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_store;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    RequestResult<uint64_t> result = rpc.on(ph)(db_name, self->m_name, record);
    if(result.success()) {
        return result.value();
    } else {
        throw std::runtime_error(result.error());
    }
    return 0;
}

uint64_t Collection::store(const Json::Value& record) const {
    return store(record.toStyledString());
}

void Collection::fetch(uint64_t id, std::string* out) const {
    if(not out) return;
    if(not self) throw std::runtime_error("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_fetch;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    RequestResult<std::string> result = rpc.on(ph)(db_name, self->m_name, id);
    if(result.success()) {
        *out = std::move(result.value());
    } else {
        throw std::runtime_error(result.error());
    }
}

void Collection::fetch(uint64_t id, Json::Value* result) const {
    if(not result) return;
    std::string str;
    fetch(id, &str);
    
    std::string errors;
    bool parsingSuccessful = self->m_json_reader->parse(str.c_str(),
            str.c_str() + str.size(),
            result,
            &errors);

    if(!parsingSuccessful) {
        throw std::runtime_error(errors);
    }
}

void Collection::filter(const std::string& filterCode, std::string* out) const {
    if(not self) throw std::runtime_error("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_filter;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    RequestResult<std::string> result = rpc.on(ph)(db_name, self->m_name, filterCode);
    if(result.success()) {
        if(out) *out = std::move(result.value());
    } else {
        throw std::runtime_error(result.error());
    }
}

void Collection::filter(const std::string& filterCode, Json::Value* result) const {
    std::string str;
    if(result)
        filter(filterCode, &str);
    else
        filter(filterCode, static_cast<std::string*>(nullptr));

    if(result) {
        std::string errors;
        bool parsingSuccessful = self->m_json_reader->parse(str.c_str(),
                                               str.c_str() + str.size(),
                                               result,
                                               &errors);
        if(!parsingSuccessful) {
            throw std::runtime_error(errors);
        }
    }
}

void Collection::update(uint64_t id, const std::string& record) const {
    if(not self) throw std::runtime_error("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_update;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    RequestResult<bool> result = rpc.on(ph)(db_name, self->m_name, id, record);
    if(result.success()) return;
    else throw std::runtime_error(result.error());
}

void Collection::update(uint64_t id, const Json::Value& record) const {
    update(id, record.toStyledString());
}

void Collection::all(std::string* out) const {
    if(not self) throw std::runtime_error("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_all;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    RequestResult<std::string> result = rpc.on(ph)(db_name, self->m_name);
    if(result.success()) {
        if(out) *out = std::move(result.value());
    } else {
        throw std::runtime_error(result.error());
    }
}

void Collection::all(Json::Value* result) const {
    std::string str;
    if(result) all(&str);
    else all(static_cast<std::string*>(nullptr));
    if(result) {
        std::string errors;
        bool parsingSuccessful = self->m_json_reader->parse(str.c_str(),
                                               str.c_str() + str.size(),
                                               result,
                                               &errors);
        if(!parsingSuccessful) {
            throw std::runtime_error(errors);
        }
    }
}

uint64_t Collection::last_record_id() const {
    if(not self) throw std::runtime_error("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_last_id;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    RequestResult<uint64_t> result = rpc.on(ph)(db_name, self->m_name);
    if(not result.success())
        throw std::runtime_error(result.error());
    return result.value();
}

size_t Collection::size() const {
    if(not self) throw std::runtime_error("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_size;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    RequestResult<size_t> result = rpc.on(ph)(db_name, self->m_name);
    if(not result.success())
        throw std::runtime_error(result.error());
    return result.value();
}

void Collection::erase(uint64_t id) const {
    if(not self) throw std::runtime_error("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_erase;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    RequestResult<bool> result = rpc.on(ph)(db_name, self->m_name);
    if(not result.success()) {
        throw std::runtime_error(result.error());
    }
}

}
