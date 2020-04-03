/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_DATABASE_HPP
#define __SONATA_DATABASE_HPP

#include <thallium.hpp>
#include <memory>
#include <unordered_set>
#include <sonata/Collection.hpp>
#include <sonata/Exception.hpp>

namespace sonata {

namespace tl = thallium;

class Client;
class DatabaseImpl;

/**
 * @brief A Database object is a handle for a remote database
 * on a server. It enables creating and opening collections.
 */
class Database {

    friend class Client;

    public:

    /**
     * @brief Constructor. The resulting Database handle will be invalid.
     */
    Database();

    /**
     * @brief Copy-constructor.
     */
    Database(const Database&);

    /**
     * @brief Move-constructor.
     */
    Database(Database&&);

    /**
     * @brief Copy-assignment operator.
     */
    Database& operator=(const Database&);

    /**
     * @brief Move-assignment operator.
     */
    Database& operator=(Database&&);

    /**
     * @brief Destructor.
     */
    ~Database();

    /**
     * @brief Creates a collection.
     *
     * @param collectionName Name of the collection to create.
     *
     * @return A valid Collection instance pointing to the new collection.
     */
    Collection create(const std::string& collectionName) const;

    /**
     * @brief Checks if a collection exists.
     *
     * @param collectionName Name of the collection.
     *
     * @return true if the collection exists, false otherwise.
     */
    bool exists(const std::string& collectionName) const;

    /**
     * @brief Opens the specified collection. The collection
     * must exist beforehand.
     *
     * @param collectionName Name of the collection.
     *
     * @return A valid Collection instance pointing to the collection.
     */
    Collection open(const std::string& collectionName) const;

    /**
     * @brief Destroys the collection. The collection must exist.
     *
     * @param collectionName Name of the collection.
     */
    void drop(const std::string& collectionName) const;

    /**
     * @brief Sends a code to execute on the database. The output
     * set indicates which variables should then be extracted and
     * returned.
     *
     * @param code Code to send to execute on the database.
     * @param vars Set of variables to extract after execution.
     * @param result Resuling map from variable names to their content.
     */
    void execute(const std::string& code,
                 const std::unordered_set<std::string>& vars,
                 std::unordered_map<std::string,std::string>* result) const;

    /**
     * @brief Sends a code to execute on the database. The output
     * set indicates which variables should then be extracted and
     * returned.
     *
     * @param code Code to send to execute on the database.
     * @param vars Set of variables to extract after execution.
     * @param result Resuling JSON object containing a mapping from
     *        variable names to their content.
     */
    void execute(const std::string& code,
                 const std::unordered_set<std::string>& vars,
                 Json::Value* result) const;

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
    Database(const std::shared_ptr<DatabaseImpl>& impl);

    std::shared_ptr<DatabaseImpl> self;
};

}

#endif
