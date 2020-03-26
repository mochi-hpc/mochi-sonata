#include "sonata/Admin.hpp"

#include "AdminImpl.hpp"
#include "RequestResult.hpp"

#include <thallium/serialization/stl/string.hpp>

namespace tl = thallium;

namespace sonata {

Admin::Admin(tl::engine& engine, const std::string& token)
: self(std::make_shared<AdminImpl>(engine, token)) {}

Admin::Admin(Admin&& other) = default;

Admin& Admin::operator=(Admin&& other) = default;

Admin::Admin(const Admin& other) = default;

Admin& Admin::operator=(const Admin& other) = default;


Admin::~Admin() = default;

Admin::operator bool() const {
    return static_cast<bool>(self);
}

void Admin::attachDatabase(const std::string& address,
                           uint16_t provider_id,
                           const std::string& db_name,
                           const std::string& db_type,
                           const std::string& db_config) const {
    auto endpoint  = self->m_engine.lookup(address);
    auto ph        = tl::provider_handle(endpoint, provider_id);
    RequestResult<bool> result = self->m_attach_database.on(ph)(self->m_token, db_name, db_type, db_config);
    if(not result.success()) {
        throw std::runtime_error(result.error());
    }
}

void Admin::attachDatabase(const std::string& address,
                           uint16_t provider_id,
                           const std::string& db_name,
                           const std::string& db_type,
                           const Json::Value& db_config) const {
    attachDatabase(address, provider_id, db_name, db_type, db_config.toStyledString());
}

void Admin::detachDatabase(const std::string& address,
                           uint16_t provider_id,
                           const std::string& db_name) const {
    auto endpoint  = self->m_engine.lookup(address);
    auto ph        = tl::provider_handle(endpoint, provider_id);
    RequestResult<bool> result = self->m_detach_database.on(ph)(self->m_token, db_name);
    if(not result.success()) {
        throw std::runtime_error(result.error());
    }
}

void Admin::destroyDatabase(const std::string& address,
                            uint16_t provider_id,
                            const std::string& db_name) const {
    auto endpoint  = self->m_engine.lookup(address);
    auto ph        = tl::provider_handle(endpoint, provider_id);
    RequestResult<bool> result = self->m_destroy_database.on(ph)(self->m_token, db_name);
    if(not result.success()) {
        throw std::runtime_error(result.error());
    }
}

}
