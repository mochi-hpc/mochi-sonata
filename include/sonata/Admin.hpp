/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_ADMIN_HPP
#define __SONATA_ADMIN_HPP

#include <memory>
#include <sonata/Exception.hpp>
#include <string>
#include <nlohmann/json.hpp>
#include <thallium.hpp>

namespace sonata {

namespace tl = thallium;
using nlohmann::json;

class AdminImpl;

/**
 * @brief Admin interface to a Sonata service. Enables creating
 * and destroying databases, and attaching and detaching them
 * from a provider. If Sonata providers have set up a security
 * token, operations from the Admin interface will need this
 * security token.
 */
class Admin {

public:
  /**
   * @brief Default constructor.
   */
  Admin();

  /**
   * @brief Constructor using a margo instance id.
   *
   * @param mid Margo instance id.
   */
  Admin(margo_instance_id mid);

  /**
   * @brief Constructor.
   *
   * @param engine Thallium engine.
   */
  Admin(const tl::engine &engine);

  /**
   * @brief Copy constructor.
   */
  Admin(const Admin &);

  /**
   * @brief Move constructor.
   */
  Admin(Admin &&);

  /**
   * @brief Copy-assignment operator.
   */
  Admin &operator=(const Admin &);

  /**
   * @brief Move-assignment operator.
   */
  Admin &operator=(Admin &&);

  /**
   * @brief Destructor.
   */
  ~Admin();

  /**
   * @brief Check if the Admin instance is valid.
   */
  operator bool() const;

  /**
   * @brief Creates a database on the target provider.
   * The config string must be a JSON object acceptable
   * by the desired backend's creation function.
   *
   * @param address Address of the target provider.
   * @param provider_id Provider id.
   * @param name Name of the database to create.
   * @param type Type of the database to create.
   * @param config JSON configuration for the database.
   * @param token Security token.
   */
  void createDatabase(const std::string &address, uint16_t provider_id,
                      const std::string &name, const std::string &type,
                      const std::string &config,
                      const std::string &token = "") const;

  /**
   * @brief Creates a database on the target provider.
   * The config string must be a JSON object acceptable
   * by the desired backend's creation function.
   *
   * @param address Address of the target provider.
   * @param provider_id Provider id.
   * @param name Name of the database to create.
   * @param type Type of the database to create.
   * @param config JSON configuration for the database.
   * @param token Security token.
   */
  void createDatabase(const std::string &address, uint16_t provider_id,
                      const std::string &name, const std::string &type,
                      const char *config, const std::string &token = "") const {
    createDatabase(address, provider_id, name, type, std::string(config));
  }

  /**
   * @brief Creates a database on the target provider.
   * The config object must be a JSON object acceptable
   * by the desired backend's creation function.
   *
   * @param address Address of the target provider.
   * @param provider_id Provider id.
   * @param name Name of the database to create.
   * @param type Type of the database to create.
   * @param config JSON configuration for the database.
   * @param token Security token.
   */
  void createDatabase(const std::string &address, uint16_t provider_id,
                      const std::string &name, const std::string &type,
                      const json &config,
                      const std::string &token = "") const;

  /**
   * @brief Attaches an existing database to the target provider.
   * The config string must be a JSON object acceptable
   * by the desired backend's attach function.
   *
   * @param address Address of the target provider.
   * @param provider_id Provider id.
   * @param name Name of the database to create.
   * @param type Type of the database to create.
   * @param config JSON configuration for the database.
   * @param token Security token.
   */
  void attachDatabase(const std::string &address, uint16_t provider_id,
                      const std::string &name, const std::string &type,
                      const std::string &config,
                      const std::string &token = "") const;

  /**
   * @brief Attaches an existing database to the target provider.
   * The config object must be a JSON object acceptable
   * by the desired backend's attach function.
   *
   * @param address Address of the target provider.
   * @param provider_id Provider id.
   * @param name Name of the database to create.
   * @param type Type of the database to create.
   * @param config JSON configuration for the database.
   * @param token Security token.
   */
  void attachDatabase(const std::string &address, uint16_t provider_id,
                      const std::string &name, const std::string &type,
                      const json &config,
                      const std::string &token = "") const;

  /**
   * @brief Detach an attached database from the target provider.
   *
   * @param address Address of the target provider.
   * @param provider_id Provider id.
   * @param name Name of the database to detach.
   * @param token Security token.
   */
  void detachDatabase(const std::string &address, uint16_t provider_id,
                      const std::string &name,
                      const std::string &token = "") const;

  /**
   * @brief Destroys an attached database from the target provider.
   *
   * @param address Address of the target provider.
   * @param provider_id Provider id.
   * @param name Name of the database to destroy.
   * @param token Security token.
   */
  void destroyDatabase(const std::string &address, uint16_t provider_id,
                       const std::string &name,
                       const std::string &token = "") const;

  /**
   * @brief Returns the list of database names managed by the target provider.
   *
   * @param address Address of the target provider.
   * @param provider_id Provider id.
   * @param token Security token.
   */
  std::vector<std::string> listDatabases(const std::string &address,
                                         uint16_t provider_id,
                                         const std::string &token = "") const;

  /**
   * @brief Shuts down the target server. The Thallium engine
   * used by the server must have remote shutdown enabled.
   *
   * @param address Address of the server to shut down.
   */
  void shutdownServer(const std::string &address) const;

private:
  std::shared_ptr<AdminImpl> self;
};

} // namespace sonata

#endif
