#include "sonata/Admin.hpp"

#include "AdminImpl.hpp"

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

bool Admin::createDatabase(const std::string& address,
                           uint16_t provider_id,
                           const std::string& db_name,
                           const std::string& db_path) const {
    auto endpoint  = self->m_engine.lookup(address);
    auto ph        = tl::provider_handle(endpoint, provider_id);
    bool ok = self->m_create_database.on(ph)(self->m_token, db_name, db_path);
    return ok;
}

}
