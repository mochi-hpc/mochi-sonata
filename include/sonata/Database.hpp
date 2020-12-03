/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_DATABASE_HPP
#define __SONATA_DATABASE_HPP

#include <json/json.h>
#include <memory>
#include <sonata/Client.hpp>
#include <sonata/Collection.hpp>
#include <sonata/Exception.hpp>
#include <thallium.hpp>
#include <unordered_set>

namespace sonata {

namespace tl = thallium;

class Client;
class DatabaseImpl;
class Collection;

/**
 * @brief A Database object is a handle for a remote database
 * on a server. It enables creating and opening collections.
 */
class Database {

  friend class Client;
  friend class Collection;

public:
  /**
   * @brief Constructor. The resulting Database handle will be invalid.
   */
  Database();

  /**
   * @brief Copy-constructor.
   */
  Database(const Database &);

  /**
   * @brief Move-constructor.
   */
  Database(Database &&);

  /**
   * @brief Copy-assignment operator.
   */
  Database &operator=(const Database &);

  /**
   * @brief Move-assignment operator.
   */
  Database &operator=(Database &&);

  /**
   * @brief Destructor.
   */
  ~Database();

  /**
   * @brief Returns the client this database has been opened with.
   */
  Client client() const;

  /**
   * @brief Creates a collection.
   *
   * @param collectionName Name of the collection to create.
   *
   * @return A valid Collection instance pointing to the new collection.
   */
  Collection create(const std::string &collectionName) const;

  /**
   * @brief Checks if a collection exists.
   *
   * @param collectionName Name of the collection.
   *
   * @return true if the collection exists, false otherwise.
   */
  bool exists(const std::string &collectionName) const;

  /**
   * @brief Opens the specified collection. If "check" is set to true,
   * an RPC will be issued to check that the collection exists. The
   * function will throw an exception if it does not. You may set "check"
   * to false if you know for sure that the collection exists.
   *
   * @param collectionName Name of the collection.
   * @param check Checks if the Collection exists by issuing an RPC.
   *
   * @return A valid Collection instance pointing to the collection.
   */
  Collection open(const std::string &collectionName, bool check = true) const;

  /**
   * @brief Destroys the collection. The collection must exist.
   *
   * @param collectionName Name of the collection.
   */
  void drop(const std::string &collectionName) const;

  /**
   * @brief Sends a code to execute on the database. The output
   * set indicates which variables should then be extracted and
   * returned.
   *
   * @param code Code to send to execute on the database.
   * @param vars Set of variables to extract after execution.
   * @param result Resuling map from variable names to their content.
   * @param commit Whether to commit changes to storage.
   */
  void execute(const std::string &code,
               const std::unordered_set<std::string> &vars,
               std::unordered_map<std::string, std::string> *result,
               bool commit = false) const;

  /**
   * @brief Sends a code to execute on the database. The output
   * set indicates which variables should then be extracted and
   * returned.
   *
   * @param code Code to send to execute on the database.
   * @param vars Set of variables to extract after execution.
   * @param result Resuling JSON object containing a mapping from
   *        variable names to their content.
   * @param commit Whether to commit changes to storage.
   */
  void execute(const std::string &code,
               const std::unordered_set<std::string> &vars, Json::Value *result,
               bool commit = false) const;

  /**
   * @brief Commit any changes made to the database.
   */
  void commit() const;

  /**
   * @brief Checks if the Database instance is valid.
   */
  operator bool() const;

private:
  /**
   * @brief Constructor is private. Use a Client object
   * to create a Database instance.
   *
   * @param impl Pointer to implementation.
   */
  Database(const std::shared_ptr<DatabaseImpl> &impl);

  std::shared_ptr<DatabaseImpl> self;
};

} // namespace sonata

#endif
