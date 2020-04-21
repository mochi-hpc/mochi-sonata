/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "sonata/Collection.hpp"
#include "sonata/RequestResult.hpp"
#include "sonata/Exception.hpp"

#include "AsyncRequestImpl.hpp"
#include "ClientImpl.hpp"
#include "DatabaseImpl.hpp"
#include "CollectionImpl.hpp"

#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace sonata {

Collection::Collection() = default;

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

Database Collection::database() const {
    return Database(self->m_database);
}

void Collection::store(const Json::Value& record, uint64_t* id, AsyncRequest* req) const {
    store(record.toStyledString(), id, req);
}

void Collection::store(const std::string& record, uint64_t* id, AsyncRequest* req) const {
    if(not self) throw Exception("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_store;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    auto async_response = rpc.on(ph).async(db_name, self->m_name, record);
    auto async_request_impl = std::make_shared<AsyncRequestImpl>(std::move(async_response));
    async_request_impl->m_wait_callback = [id](AsyncRequestImpl& async_request_impl) {
        RequestResult<uint64_t> result = async_request_impl.m_async_response.wait();
        if(result.success()) {
            if(id) *id = result.value();
        } else {
            throw Exception(result.error());
        }
    };
    if(req)
        *req = AsyncRequest(std::move(async_request_impl));
    else
        AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::fetch(uint64_t id, std::string* out, AsyncRequest* req) const {
    if(not out) return;
    if(not self) throw Exception("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_fetch;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    auto async_response = rpc.on(ph).async(db_name, self->m_name, id);
    auto async_request_impl = std::make_shared<AsyncRequestImpl>(std::move(async_response));
    async_request_impl->m_wait_callback = [out](AsyncRequestImpl& async_request_impl) {
        RequestResult<std::string> result = async_request_impl.m_async_response.wait();
        if(result.success()) {
            *out = std::move(result.value());
        } else {
            throw Exception(result.error());
        }
    };
    if(req)
        *req = AsyncRequest(std::move(async_request_impl));
    else
        AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::fetch(uint64_t id, Json::Value* out, AsyncRequest* req) const {
    if(not out) return;
    if(not self) throw Exception("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_fetch;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    auto async_response = rpc.on(ph).async(db_name, self->m_name, id);
    auto async_request_impl = std::make_shared<AsyncRequestImpl>(std::move(async_response));
    async_request_impl->m_wait_callback = [out, self=self](AsyncRequestImpl& async_request_impl) {
        RequestResult<std::string> result = async_request_impl.m_async_response.wait();
        if(result.success()) {
            std::string errors;
            bool parsingSuccessful = self->m_json_reader->parse(result.value().c_str(),
                                                result.value().c_str() + result.value().size(),
                                                out,
                                                &errors);
            if(!parsingSuccessful) {
                throw Exception(errors);
            }
        } else {
            throw Exception(result.error());
        }
    };
    if(req)
        *req = AsyncRequest(std::move(async_request_impl));
    else
        AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::filter(const std::string& filterCode, std::vector<std::string>* out, AsyncRequest* req) const {
    if(not self) throw Exception("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_filter;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    auto async_response = rpc.on(ph).async(db_name, self->m_name, filterCode);
    auto async_request_impl = std::make_shared<AsyncRequestImpl>(std::move(async_response));
    async_request_impl->m_wait_callback = [out](AsyncRequestImpl& async_request_impl) {
        RequestResult<std::vector<std::string>> result = async_request_impl.m_async_response.wait();
        if(result.success()) {
            if(out) *out = std::move(result.value());
        } else {
            throw Exception(result.error());
        }
    };
    if(req)
        *req = AsyncRequest(std::move(async_request_impl));
    else
        AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::filter(const std::string& filterCode, Json::Value* out, AsyncRequest* req) const {
    if(not self) throw Exception("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_filter;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    auto async_response = rpc.on(ph).async(db_name, self->m_name, filterCode);
    auto async_request_impl = std::make_shared<AsyncRequestImpl>(std::move(async_response));
    async_request_impl->m_wait_callback = [out, self=self](AsyncRequestImpl& async_request_impl) {
        RequestResult<std::vector<std::string>> result = async_request_impl.m_async_response.wait();
        if(result.success()) {
            if(!out) return;
            Json::Value tmp_array;
            for(unsigned i=0; i < result.value().size(); i++) {
                std::string errors;
                Json::Value tmp;
                bool parsingSuccessful = self->m_json_reader->parse(result.value()[i].c_str(),
                        result.value()[i].c_str() + result.value()[i].size(),
                        &tmp,
                        &errors);
                if(!parsingSuccessful) {
                    throw Exception(errors);
                }
                tmp_array[i] = std::move(tmp);
            }
            *out = std::move(tmp_array);
        } else {
            throw Exception(result.error());
        }
    };
    if(req)
        *req = AsyncRequest(std::move(async_request_impl));
    else
        AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::update(uint64_t id, const std::string& record, AsyncRequest* req) const {
    if(not self) throw Exception("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_update;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    auto async_response = rpc.on(ph).async(db_name, self->m_name, id, record);
    auto async_request_impl = std::make_shared<AsyncRequestImpl>(std::move(async_response));
    async_request_impl->m_wait_callback = [](AsyncRequestImpl& async_request_impl) {
        RequestResult<bool> result = async_request_impl.m_async_response.wait();
        if(!result.success()) {
            throw Exception(result.error());
        }
    };
    if(req)
        *req = AsyncRequest(std::move(async_request_impl));
    else
        AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::update(uint64_t id, const Json::Value& record, AsyncRequest* req) const {
    update(id, record.toStyledString(), req);
}

void Collection::all(std::vector<std::string>* out, AsyncRequest* req) const {
    if(not out) return;
    if(not self) throw Exception("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_all;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    auto async_response = rpc.on(ph).async(db_name, self->m_name);
    auto async_request_impl = std::make_shared<AsyncRequestImpl>(std::move(async_response));
    async_request_impl->m_wait_callback = [out](AsyncRequestImpl& async_request_impl) {
        RequestResult<std::vector<std::string>> result = async_request_impl.m_async_response.wait();
        if(result.success()) {
            *out = std::move(result.value());
        } else {
            throw Exception(result.error());
        }
    };
    if(req)
        *req = AsyncRequest(std::move(async_request_impl));
    else
        AsyncRequest(std::move(async_request_impl)).wait();
}

void Collection::all(Json::Value* out, AsyncRequest* req) const {
    if(not self) throw Exception("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_all;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    auto async_response = rpc.on(ph).async(db_name, self->m_name);
    auto async_request_impl = std::make_shared<AsyncRequestImpl>(std::move(async_response));
    async_request_impl->m_wait_callback = [out, self=self](AsyncRequestImpl& async_request_impl) {
        RequestResult<std::vector<std::string>> result = async_request_impl.m_async_response.wait();
        if(result.success()) {
            if(!out) return;
            Json::Value tmp_array;
            for(unsigned i=0; i < result.value().size(); i++) {
                std::string errors;
                Json::Value tmp;
                bool parsingSuccessful = self->m_json_reader->parse(result.value()[i].c_str(),
                        result.value()[i].c_str() + result.value()[i].size(),
                        &tmp,
                        &errors);
                if(!parsingSuccessful) {
                    throw Exception(errors);
                }
                tmp_array[i] = std::move(tmp);
            }
            *out = std::move(tmp_array);
        } else {
            throw Exception(result.error());
        }
    };
    if(req)
        *req = AsyncRequest(std::move(async_request_impl));
    else
        AsyncRequest(std::move(async_request_impl)).wait();
}

uint64_t Collection::last_record_id() const {
    if(not self) throw Exception("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_last_id;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    RequestResult<uint64_t> result = rpc.on(ph)(db_name, self->m_name);
    if(not result.success())
        throw Exception(result.error());
    return result.value();
}

size_t Collection::size() const {
    if(not self) throw Exception("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_size;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    RequestResult<size_t> result = rpc.on(ph)(db_name, self->m_name);
    if(not result.success())
        throw Exception(result.error());
    return result.value();
}

void Collection::erase(uint64_t id, AsyncRequest* req) const {
    if(not self) throw Exception("Invalid sonata::Collection object");
    auto& rpc = self->m_database->m_client->m_coll_erase;
    auto& ph  = self->m_database->m_ph;
    auto& db_name = self->m_database->m_name;
    auto async_response = rpc.on(ph).async(db_name, self->m_name, id);
    auto async_request_impl = std::make_shared<AsyncRequestImpl>(std::move(async_response));
    async_request_impl->m_wait_callback = [](AsyncRequestImpl& async_request_impl) {
        RequestResult<bool> result = async_request_impl.m_async_response.wait();
        if(!result.success()) {
            throw Exception(result.error());
        }
    };
    if(req)
        *req = AsyncRequest(std::move(async_request_impl));
    else
        AsyncRequest(std::move(async_request_impl)).wait();
}

}
