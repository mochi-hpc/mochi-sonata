/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "sonata/Provider.hpp"

#include "ProviderImpl.hpp"

#include <thallium/serialization/stl/string.hpp>

namespace tl = thallium;

namespace {
    void delete_engine(void* uargs) {
        delete static_cast<tl::engine*>(uargs);
    }
}

namespace sonata {

Provider::Provider(tl::engine& engine, uint16_t provider_id, const tl::pool& p)
: self(std::make_shared<ProviderImpl>(engine, provider_id, p)) {
    engine.push_finalize_callback(this, [p=this]() { p->self.reset(); });
}

Provider::Provider(margo_instance_id mid, uint16_t provider_id, const tl::pool& p) {
    auto engine = new tl::engine(mid);
    self = std::make_shared<ProviderImpl>(*engine, provider_id, p);
    engine->push_finalize_callback(this, [p=this]() { p->self.reset(); });
    margo_push_finalize_callback(mid, delete_engine, static_cast<void*>(engine));
}

Provider::Provider(Provider&& other) {
    other.self->get_engine().pop_finalize_callback(this);
    self = std::move(other.self);
    self->get_engine().push_finalize_callback(this, [p=this]() { p->self.reset(); });
}

Provider::~Provider() {
    if(self) {
        self->get_engine().pop_finalize_callback(this);
    }
}

Provider::operator bool() const {
    return static_cast<bool>(self);
}

void Provider::setSecurityToken(const std::string& token) {
    if(self) self->m_token = token;
}

}
