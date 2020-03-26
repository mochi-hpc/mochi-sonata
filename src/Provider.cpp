#include "sonata/Provider.hpp"

#include "ProviderImpl.hpp"

#include <thallium/serialization/stl/string.hpp>

namespace tl = thallium;

namespace sonata {

Provider::Provider(tl::engine& engine, uint16_t provider_id, const tl::pool& p)
: self(std::make_shared<ProviderImpl>(engine, provider_id, p)) {
    engine.push_finalize_callback(this, [p=this]() { p->self.reset(); });
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

}
