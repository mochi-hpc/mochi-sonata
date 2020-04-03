/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_BACKEND_HPP
#define __SONATA_BACKEND_HPP

#include <sonata/RequestResult.hpp>
#include <unordered_set>
#include <unordered_map>
#include <functional>
#include <json/json.h>

/**
 * @brief Helper class to register backend types into the backend factory.
 */
template<typename BackendType>
class __SonataBackendRegistration;

namespace sonata {

/**
 * @brief Interface for database backends. To build a new backend,
 * implement a class that inherits from Backend, and put
 * SONATA_REGISTER_BACKEND(mybackend, MyBackend); in a cpp file
 * that includes your backend class' header file.
 *
 * Your backend class should also have two static functions to
 * respectively create and open a backend database:
 *
 * std::unique_ptr<Backend> create(const Json::Value& config)
 * std::unique_ptr<Backend> attach(const Json::Value& config)
 */
class Backend {
    
    public:

    /**
     * @brief Constructor.
     */
    Backend() = default;

    /**
     * @brief Move-constructor.
     */
    Backend(Backend&&) = default;

    /**
     * @brief Copy-constructor.
     */
    Backend(const Backend&) = default;

    /**
     * @brief Move-assignment operator.
     */
    Backend& operator=(Backend&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    Backend& operator=(const Backend&) = default;

    /**
     * @brief Destructor.
     */
    virtual ~Backend() = default;

    /**
     * @brief Executes Jx9 code on the Database. The variable 
     * names specified in vars will then be extracted and returned.
     *
     * @param code Code to execute.
     * @param vars Variables to return.
     *
     * @return a RequestResult<std::unordered_map<std::string,std::string>>
     * instance.
     */
    virtual RequestResult<std::unordered_map<std::string,std::string>>
        execute(const std::string& code,
                const std::unordered_set<std::string>& vars) = 0;

    /**
     * @brief Creates a new collection in the database.
     *
     * @param coll_name Name of the collection.
     *
     * @return a RequestResult<bool> instance.
     */
    virtual RequestResult<bool> createCollection(
            const std::string& coll_name) = 0;

    /**
     * @brief Checks if a collection exists.
     *
     * @param coll_name Name of the collection.
     *
     * @return a RequestResult<bool> instance.
     */
    virtual RequestResult<bool> openCollection(
            const std::string& coll_name) = 0;

    /**
     * @brief Destroys a collection
     *
     * @param coll_name Name of the collection.
     *
     * @return a RequestResult<bool> instance.
     */
    virtual RequestResult<bool> dropCollection(
            const std::string& coll_name) = 0;

    /**
     * @brief Stores a record into the collection.
     * The record should be a valid JSON object.
     *
     * @param coll_name Name of the collection.
     * @param record Record to store.
     *
     * @return a RequestResult<uint64_t> instance
     * containing the record id if successful.
     */
    virtual RequestResult<uint64_t> store(
            const std::string& coll_name,
            const std::string& record) = 0;

    /**
     * @brief Fetches a particular record by its id.
     *
     * @param coll_name Name of the collection.
     * @param record_id Record id.
     *
     * @return a RequestResult<std::string> instance.
     * containing the content of the record if successful.
     */
    virtual RequestResult<std::string> fetch(
            const std::string& coll_name,
            uint64_t record_id) = 0;

    /**
     * @brief Returns an array of records matching a give
     * Jx9 filter. The filter should be expressed as a string
     * containing a function. For example:
     *
     * "function($user) { return $user.age > 30; }"
     *
     * @param coll_name Name of the collection.
     * @param filter_code Code of the Jx9 function.
     *
     * @return a RequestResult<std::vector<std::string>>
     * instance containing the result of the request.
     */
    virtual RequestResult<std::vector<std::string>> filter(
            const std::string& coll_name,
            const std::string& filter_code) = 0;

    /**
     * @brief Updates an existing record with the new content.
     *
     * @param coll_name Name of the collection.
     * @param record_id Record to update.
     * @param new_content New content of the record.
     *
     * @return a RequestResult<bool> instance indicating
     * whether the update was successful.
     */
    virtual RequestResult<bool> update(
            const std::string& coll_name,
            uint64_t record_id,
            const std::string& new_content) = 0;

    /**
     * @brief Returns all the records in the collection.
     *
     * @param coll_name Name of the collection.
     *
     * @return a RequestResult<std::vector<std::string>> instance
     * containing all the records as strings, if successful.
     */
    virtual RequestResult<std::vector<std::string>> all(
            const std::string& coll_name) = 0;

    /**
     * @brief Returns the last record id stored in the collection.
     *
     * @param coll_name Name of the collection.
     *
     * @return a RequestResult<uint64_t> instance containing
     * the last record id, if successful.
     */
    virtual RequestResult<uint64_t> lastID(
            const std::string& coll_name) = 0;

    /**
     * @brief Returns the size of the collection.
     *
     * @param coll_name Name of the collection.
     *
     * @return a RequestResult<size_t> instance containing
     * the size of the collection.
     */
    virtual RequestResult<size_t> size(
            const std::string& coll_name) = 0;

    /**
     * @brief Erases the specified record from the collection.
     *
     * @param coll_name Name of the collection.
     * @param record_id Record to erase.
     *
     * @return a RequestResult<bool> instance.
     */
    virtual RequestResult<bool> erase(
            const std::string& coll_name,
            uint64_t record_id) = 0;

    /**
     * @brief Destroys the underlying database resources.
     *
     * @return a RequestResult<bool> instance indicating
     * whether the database was successfully destroyed.
     */
    virtual RequestResult<bool> destroy() = 0;

};

/**
 * @brief The BackendFactory contains function to create
 * or attach databases.
 */
class BackendFactory {

    template<typename BackendType>
    friend class ::__SonataBackendRegistration;

    public:

    BackendFactory() = delete;

    /**
     * @brief Creates a database and returns a unique_ptr to the created 
     * backend instance.
     *
     * @param backend_name Name of the backend to use.
     * @param config Configuration object to pass to the backend's create function.
     *
     * @return a unique_ptr to the created Backend.
     */
    static std::unique_ptr<Backend> createBackend(const std::string& backend_name,
                                                  const Json::Value& config);

    /**
     * @brief Opens an existing database and returns a unique_ptr to the
     * created backend instance.
     *
     * @param backend_name Name of the backend to use.
     * @param config Configuration object to pass to the backend's attach function.
     *
     * @return a unique_ptr to the created Backend.
     */
    static std::unique_ptr<Backend> attachBackend(const std::string& backend_name,
                                                  const Json::Value& config);

    private:

    static std::unordered_map<std::string,
                std::function<std::unique_ptr<Backend>(const Json::Value&)>> create_fn;
    
    static std::unordered_map<std::string,
                std::function<std::unique_ptr<Backend>(const Json::Value&)>> attach_fn;
};

} // namespace sonata


#define SONATA_REGISTER_BACKEND(__backend_name, __backend_type) \
    static __SonataBackendRegistration<__backend_type> __sonata ## __backend_name ## _backend( #__backend_name )

template<typename BackendType>
class __SonataBackendRegistration {
    
    public:

    __SonataBackendRegistration(const std::string& backend_name)
    {
        sonata::BackendFactory::create_fn[backend_name] = [](const Json::Value& config) {
            return BackendType::create(config);
        };
        sonata::BackendFactory::attach_fn[backend_name] = [](const Json::Value& config) {
            return BackendType::attach(config);
        };
    }
};

#endif
