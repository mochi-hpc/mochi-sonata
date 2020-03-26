#include "sonata/Provider.hpp"

#include "ProviderImpl.hpp"

#include <thallium/serialization/stl/string.hpp>

namespace tl = thallium;

namespace sonata {

Provider::Provider(tl::engine& engine, uint16_t provider_id, const tl::pool& p)
: self(std::make_shared<ProviderImpl>(engine, provider_id, p)) {}

Provider::Provider(Provider&& other) = default;

Provider& Provider::operator=(Provider&& other) = default;

Provider::Provider(const Provider& other) = default;

Provider& Provider::operator=(const Provider& other) = default;

Provider::~Provider() = default;

Provider::operator bool() const {
    return static_cast<bool>(self);
}

}
