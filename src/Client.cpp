/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "sonata/Client.hpp"
#include "sonata/Collection.hpp"
#include "sonata/Database.hpp"
#include "sonata/Exception.hpp"
#include "sonata/RequestResult.hpp"

#include "ClientImpl.hpp"
#include "DatabaseImpl.hpp"

#include <thallium/serialization/stl/string.hpp>

namespace tl = thallium;

namespace sonata {

Client::Client() = default;

Client::Client(const tl::engine &engine)
    : self(std::make_shared<ClientImpl>(engine)) {}

Client::Client(margo_instance_id mid)
    : self(std::make_shared<ClientImpl>(mid)) {}

Client::Client(const std::shared_ptr<ClientImpl> &impl) : self(impl) {}

Client::Client(Client &&other) = default;

Client &Client::operator=(Client &&other) = default;

Client::Client(const Client &other) = default;

Client &Client::operator=(const Client &other) = default;

Client::~Client() = default;

const tl::engine &Client::engine() const { return self->m_engine; }

Client::operator bool() const { return static_cast<bool>(self); }

ProviderHandle Client::createProviderHandle(const std::string &address,
                                            uint16_t provider_id) const {
  tl::endpoint endpoint;
  while (endpoint.is_null()) {
    try {
      endpoint = self->m_engine.lookup(address);
    } catch (const tl::margo_exception &ex) {
      // TODO when thallium provides a way to get the error
      // code from a margo_exception, change the code bellow
      // to compare the exception code instead of searching
      // of HG_AGAIN in the error message
      auto s = std::string(ex.what());
      if (s.find("HG_AGAIN") == std::string::npos)
        throw;
    }
  }
  return ProviderHandle(endpoint, provider_id);
}

ProviderHandle Client::createProviderHandle(hg_addr_t address,
                                            uint16_t provider_id) const {
  return ProviderHandle(self->m_engine, address, provider_id, false);
}

Database Client::open(const ProviderHandle &ph, const std::string &db_name,
                      bool check) const {
  RequestResult<bool> result;
  result.success() = true;
  if (check) {
    result = self->m_open_database.on(ph)(db_name);
  }
  if (result.success()) {
    auto db_impl = std::make_shared<DatabaseImpl>(self, ph, db_name);
    return Database(db_impl);
  } else {
    throw Exception(result.error());
    return Database(nullptr);
  }
}

Database Client::open(const std::string &address, uint16_t provider_id,
                      const std::string &db_name, bool check) const {
  auto ph = createProviderHandle(address, provider_id);
  return open(ph, db_name, check);
}

} // namespace sonata
