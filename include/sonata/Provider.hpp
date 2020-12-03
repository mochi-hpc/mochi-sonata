/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_PROVIDER_HPP
#define __SONATA_PROVIDER_HPP

#include <memory>
#include <thallium.hpp>

namespace sonata {

namespace tl = thallium;

class ProviderImpl;

/**
 * @brief A Provider is an object that can receive RPCs
 * and dispatch them to specific databases.
 */
class Provider {

public:
  /**
   * @brief Constructor.
   *
   * @param engine Thallium engine to use to receive RPCs.
   * @param provider_id Provider id.
   * @param config JSON-formatted configuration.
   * @param pool Argobots pool to use to handle RPCs.
   */
  Provider(tl::engine &engine, uint16_t provider_id = 0,
           const std::string &config = std::string(),
           const tl::pool &pool = tl::pool());

  /**
   * @brief Constructor.
   *
   * @param mid Margo instance id to use to receive RPCs.
   * @param provider_id Provider id.
   * @param config JSON-formatted configuration.
   * @param pool Argobots pool to use to handle RPCs.
   */
  Provider(margo_instance_id mid, uint16_t provider_id = 0,
           const std::string &config = std::string(),
           const tl::pool &pool = tl::pool());

  /**
   * @brief Copy-constructor is deleted.
   */
  Provider(const Provider &) = delete;

  /**
   * @brief Move-constructor.
   */
  Provider(Provider &&);

  /**
   * @brief Copy-assignment operator is deleted.
   */
  Provider &operator=(const Provider &) = delete;

  /**
   * @brief Move-assignment operator is deleted.
   */
  Provider &operator=(Provider &&) = delete;

  /**
   * @brief Destructor.
   */
  ~Provider();

  /**
   * @brief Get the internal configuration as a JSON string.
   *
   * @return The internal configuration.
   */
  std::string getConfig() const;

  /**
   * @brief Sets a security string that should be provided
   * by Admin RPCs to accept them.
   *
   * @param token Security token to set.
   */
  void setSecurityToken(const std::string &token);

  /**
   * @brief Checks whether the Provider instance is valid.
   */
  operator bool() const;

private:
  std::shared_ptr<ProviderImpl> self;
};

} // namespace sonata

#endif
