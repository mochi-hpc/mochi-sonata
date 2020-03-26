#include "sonata/Collection.hpp"

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

bool Collection::store(const std::string& record, uint64_t* id) const {
    if(not self) throw std::runtime_error("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_store;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    std::pair<bool,uint64_t> ret = rpc.on(ph)(db_name, self->m_name, record);
    if(ret.first && id) {
        *id = ret.second;
    }
    return ret.first;
}

bool Collection::store(const Json::Value& record, uint64_t* id) const {
    return store(record.toStyledString(), id);
}

bool Collection::fetch(uint64_t id, std::string* result) const {
    if(not self) throw std::runtime_error("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_fetch;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    std::pair<bool,std::string> ret = rpc.on(ph)(db_name, self->m_name, id);
    if(ret.first && result) {
        *result = std::move(ret.second);
    }
    return ret.first;
}

bool Collection::fetch(uint64_t id, Json::Value* result) const {
    std::string str;
    bool b = fetch(id, &str);
    if(b && result) {
        std::string errors;
        bool parsingSuccessful = self->m_json_reader->parse(str.c_str(),
                                               str.c_str() + str.size(),
                                               result,
                                               &errors);
        if(!parsingSuccessful) {
            throw std::runtime_error(errors);
        }
    }
    return b;
}

bool Collection::filter(const std::string& filterCode, std::string* result) const {
    if(not self) throw std::runtime_error("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_filter;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    std::pair<bool,std::string> ret = rpc.on(ph)(db_name, self->m_name, filterCode);
    if(ret.first && result) {
        *result = std::move(ret.second);
    }
    return ret.first;
}

bool Collection::filter(const std::string& filterCode, Json::Value* result) const {
    std::string str;
    bool b = filter(filterCode, &str);
    if(b && result) {
        std::string errors;
        bool parsingSuccessful = self->m_json_reader->parse(str.c_str(),
                                               str.c_str() + str.size(),
                                               result,
                                               &errors);
        if(!parsingSuccessful) {
            throw std::runtime_error(errors);
        }
    }
    return b;
}

bool Collection::update(uint64_t id, const std::string& record) const {
    if(not self) throw std::runtime_error("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_update;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    bool ret = rpc.on(ph)(db_name, self->m_name, id, record);
    return ret;
}

bool Collection::update(uint64_t id, const Json::Value& record) const {
    return update(id, record.toStyledString());
}

bool Collection::all(std::string* result) const {
    if(not self) throw std::runtime_error("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_all;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    std::pair<bool,std::string> ret = rpc.on(ph)(db_name, self->m_name);
    if(ret.first && result) {
        *result = std::move(ret.second);
    }
    return ret.first;
}

bool Collection::all(Json::Value* result) const {
    std::string str;
    bool b = all(&str);
    if(b && result) {
        std::string errors;
        bool parsingSuccessful = self->m_json_reader->parse(str.c_str(),
                                               str.c_str() + str.size(),
                                               result,
                                               &errors);
        if(!parsingSuccessful) {
            throw std::runtime_error(errors);
        }
    }
    return b;
}

uint64_t Collection::last_record_id() const {
    if(not self) throw std::runtime_error("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_last_id;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    uint64_t ret = rpc.on(ph)(db_name, self->m_name);
    return ret;
}

size_t Collection::size() const {
    if(not self) throw std::runtime_error("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_size;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    size_t ret = rpc.on(ph)(db_name, self->m_name);
    return ret;
}

bool Collection::erase(uint64_t id) const {
    if(not self) throw std::runtime_error("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_erase;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    bool ret = rpc.on(ph)(db_name, self->m_name);
    return ret;
}

}
