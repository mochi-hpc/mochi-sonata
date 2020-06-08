/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "sonata/Admin.hpp"
#include "sonata/Exception.hpp"
#include "sonata/RequestResult.hpp"

#include "AdminImpl.hpp"

#include <thallium/serialization/stl/string.hpp>

namespace tl = thallium;

namespace sonata {

Admin::Admin() = default;

Admin::Admin(const tl::engine& engine)
: self(std::make_shared<AdminImpl>(engine)) {}

Admin::Admin(margo_instance_id mid)
: self(std::make_shared<AdminImpl>(mid)) {}

Admin::Admin(Admin&& other) = default;

Admin& Admin::operator=(Admin&& other) = default;

Admin::Admin(const Admin& other) = default;

Admin& Admin::operator=(const Admin& other) = default;


Admin::~Admin() = default;

Admin::operator bool() const {
    return static_cast<bool>(self);
}

void Admin::createDatabase(const std::string& address,
                           uint16_t provider_id,
                           const std::string& db_name,
                           const std::string& db_type,
                           const std::string& db_config,
                           const std::string& token) const {
    auto endpoint  = self->m_engine.lookup(address);
    auto ph        = tl::provider_handle(endpoint, provider_id);
    RequestResult<bool> result = self->m_create_database.on(ph)(token, db_name, db_type, db_config);
    if(not result.success()) {
        throw Exception(result.error());
    }
}

void Admin::createDatabase(const std::string& address,
                           uint16_t provider_id,
                           const std::string& db_name,
                           const std::string& db_type,
                           const Json::Value& db_config,
                           const std::string& token) const {
    createDatabase(address, provider_id, db_name, db_type, db_config.toStyledString(), token);
}

void Admin::attachDatabase(const std::string& address,
                           uint16_t provider_id,
                           const std::string& db_name,
                           const std::string& db_type,
                           const std::string& db_config,
                           const std::string& token) const {
    auto endpoint  = self->m_engine.lookup(address);
    auto ph        = tl::provider_handle(endpoint, provider_id);
    RequestResult<bool> result = self->m_attach_database.on(ph)(token, db_name, db_type, db_config);
    if(not result.success()) {
        throw Exception(result.error());
    }
}

void Admin::attachDatabase(const std::string& address,
                           uint16_t provider_id,
                           const std::string& db_name,
                           const std::string& db_type,
                           const Json::Value& db_config,
                           const std::string& token) const {
    attachDatabase(address, provider_id, db_name, db_type, db_config.toStyledString(), token);
}

void Admin::detachDatabase(const std::string& address,
                           uint16_t provider_id,
                           const std::string& db_name,
                           const std::string& token) const {
    auto endpoint  = self->m_engine.lookup(address);
    auto ph        = tl::provider_handle(endpoint, provider_id);
    RequestResult<bool> result = self->m_detach_database.on(ph)(token, db_name);
    if(not result.success()) {
        throw Exception(result.error());
    }
}

void Admin::destroyDatabase(const std::string& address,
                            uint16_t provider_id,
                            const std::string& db_name,
                            const std::string& token) const {
    auto endpoint  = self->m_engine.lookup(address);
    auto ph        = tl::provider_handle(endpoint, provider_id);
    RequestResult<bool> result = self->m_destroy_database.on(ph)(token, db_name);
    if(not result.success()) {
        throw Exception(result.error());
    }
}

void Admin::shutdownServer(const std::string& address) const {
    auto ep = self->m_engine.lookup(address);
    self->m_engine.shutdown_remote_engine(ep);
}

}
