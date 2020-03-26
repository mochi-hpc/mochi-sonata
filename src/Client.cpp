#include "sonata/Client.hpp"
#include "sonata/Database.hpp"
#include "sonata/Collection.hpp"
#include "sonata/RequestResult.hpp"

#include "ClientImpl.hpp"
#include "DatabaseImpl.hpp"

#include <thallium/serialization/stl/string.hpp>

namespace tl = thallium;

namespace sonata {

Client::Client(tl::engine& engine)
: self(std::make_shared<ClientImpl>(engine)) {}

Client::Client(Client&& other) = default;

Client& Client::operator=(Client&& other) = default;

Client::Client(const Client& other) = default;

Client& Client::operator=(const Client& other) = default;


Client::~Client() = default;

Client::operator bool() const {
    return static_cast<bool>(self);
}

Database Client::open(const std::string& address,
                      uint16_t provider_id,
                      const std::string& db_name) const {
    auto endpoint  = self->m_engine.lookup(address);
    auto ph        = tl::provider_handle(endpoint, provider_id);
    RequestResult<bool> result = self->m_open_database.on(ph)(db_name);
    if(result.success()) {
        auto db_impl = std::make_shared<DatabaseImpl>(self, std::move(ph), db_name);
        return Database(db_impl);
    } else {
        throw std::runtime_error(result.error());
        return Database(nullptr);
    }
}

}
