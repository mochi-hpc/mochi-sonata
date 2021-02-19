/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "sonata/Client.hpp"
#include "sonata/Provider.hpp"
#include "sonata/ProviderHandle.hpp"
#include <bedrock/AbstractServiceFactory.hpp>

namespace tl = thallium;

class SonataFactory : public bedrock::AbstractServiceFactory {

public:
  SonataFactory() {}

  void *registerProvider(const bedrock::FactoryArgs &args) override {
    auto provider = new sonata::Provider(args.mid, args.provider_id,
                                         args.config, tl::pool(args.pool));
    return static_cast<void *>(provider);
  }

  void deregisterProvider(void *p) override {
    auto provider = static_cast<sonata::Provider *>(p);
    delete provider;
  }

  std::string getProviderConfig(void *p) override {
    auto provider = static_cast<sonata::Provider *>(p);
    return provider->getConfig();
  }

  void *initClient(const bedrock::FactoryArgs &args) override {
    return static_cast<void *>(new sonata::Client(args.mid));
  }

  void finalizeClient(void *client) override {
    delete static_cast<sonata::Client *>(client);
  }

  std::string getClientConfig(void *p) override {
    (void)p;
    return "{}";
  }

  void *createProviderHandle(void *c, hg_addr_t address,
                             uint16_t provider_id) override {
    auto client = static_cast<sonata::Client *>(c);
    auto ph = new sonata::ProviderHandle(
        client->createProviderHandle(address, provider_id));
    return static_cast<void *>(ph);
  }

  void destroyProviderHandle(void *providerHandle) override {
    auto ph = static_cast<sonata::ProviderHandle *>(providerHandle);
    delete ph;
  }

  const std::vector<bedrock::Dependency> &getProviderDependencies() override {
    static const std::vector<bedrock::Dependency> no_dependency;
    return no_dependency;
  }

  const std::vector<bedrock::Dependency> &getClientDependencies() override {
    static const std::vector<bedrock::Dependency> no_dependency;
    return no_dependency;
  }
};

BEDROCK_REGISTER_MODULE_FACTORY(sonata, SonataFactory)
