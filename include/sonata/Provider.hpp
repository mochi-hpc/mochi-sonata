#ifndef __SONATA_PROVIDER_HPP
#define __SONATA_PROVIDER_HPP

#include <thallium.hpp>
#include <memory>

namespace sonata {

namespace tl = thallium;

class ProviderImpl;

class Provider {

    public:

    Provider(tl::engine& engine,
             uint16_t provider_id = 0,
             const tl::pool& pool = tl::pool());
    
    Provider(const Provider&) = delete;

    Provider(Provider&&);

    Provider& operator=(const Provider&) = delete;

    Provider& operator=(Provider&&) = delete;

    ~Provider();

    void setSecurityToken(const std::string& token);

    operator bool() const;

    private:

    std::shared_ptr<ProviderImpl> self;
};

}

#endif
