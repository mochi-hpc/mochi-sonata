/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_COLLECTION_HPP
#define __SONATA_COLLECTION_HPP

#include <sonata/Database.hpp>
#include <json/json.h>
#include <thallium.hpp>
#include <memory>

namespace sonata {

namespace tl = thallium;

class DatabaseImpl;
class CollectionImpl;
class Database;

/**
 * @brief The Collection object is a handle to a collection
 * in a given remote database on a server. It offers function
 * to store and search documents.
 */
class Collection {

    friend class Database;

    public:

    /**
     * @brief Default constructor. The resulting collection
     * instance will be invalid.
     */
    Collection();

    /**
     * @brief Copy constructor.
     */
    Collection(const Collection&);

    /**
     * @brief Move constructor.
     */
    Collection(Collection&&);

    /**
     * @brief Copy-assignment operator.
     */
    Collection& operator=(const Collection&);

    /**
     * @brief Move-assignment operator.
     */
    Collection& operator=(Collection&&);

    /**
     * @brief Destructor.
     */
    ~Collection();

    /**
     * @brief Checks if the Collection object is valid.
     */
    operator bool() const;

    /**
     * @brief Returns the database this collection belongs to.
     */
    Database database() const;

    /**
     * @brief Stores a document into the collection.
     *
     * @param record A valid JSON-formated document.
     *
     * @return the record id of the stored document.
     */
    uint64_t store(const std::string& record) const;

    /**
     * @brief Stores a document into the collection.
     *
     * @param record A JSON object.
     *
     * @return the record id of the stored document.
     */
    uint64_t store(const Json::Value& record) const;

    /**
     * @brief Stores a document into the collection.
     *
     * @param record A valid JSON-formated document.
     *
     * @return the record id of the stored document.
     */
    uint64_t store(const char* record) const {
        return store(std::string(record));
    }

    /**
     * @brief Fetches a document by its record id.
     *
     * @param[in] id Record id.
     * @param[out] result Resulting string.
     */
    void fetch(uint64_t id,
               std::string* result) const;

    /**
     * @brief Fetches a document by its record id.
     *
     * @param[in] id Record id.
     * @param[out] result Resulting JSON object.
     */
    void fetch(uint64_t id,
               Json::Value* result) const;

    /**
     * @brief Filters the collection and returns the
     * records that match the condition. This condition should
     * be expressed as a Jx9 function returning TRUE or FALSE.
     * For example the following Jx9 function selects only
     * the records where x < 4 :
     *
     * "function($record) { return $record.x < 4; }"
     *
     * @param filterCode A Jx9 filter code.
     * @param result Resuling vector of records as strings.
     */
    void filter(const std::string& filterCode,
                std::vector<std::string>* result) const;

    /**
     * @brief Filters the collection and returns the
     * records that match the condition. This condition should
     * be expressed as a Jx9 function returning TRUE or FALSE.
     * For example the following Jx9 function selects only
     * the records where x < 4 :
     *
     * "function($record) { return $record.x < 4; }"
     *
     * @param filterCode A Jx9 filter code.
     * @param result Resuling JSON object containing the array of results.
     */
    void filter(const std::string& filterCode,
                Json::Value* result) const;

    /**
     * @brief Updates the content of a document with a new content.
     *
     * @param id Record id of the document to update.
     * @param record New document.
     */
    void update(uint64_t id,
                const Json::Value& record) const;

    /**
     * @brief Updates the content of a document with a new content.
     *
     * @param id Record id of the document to update.
     * @param record New document.
     */
    void update(uint64_t id,
                const std::string& record) const;

    /**
     * @brief Updates the content of a document with a new content.
     *
     * @param id Record id of the document to update.
     * @param record New document.
     */
    void update(uint64_t id,
                const char* record) const {
        return update(id, std::string(record));
    }

    /**
     * @brief Returns all the documents from the collection
     * as a vector of strings.
     *
     * @param result All the documents from the collection.
     */
    void all(std::vector<std::string>* result) const;

    /**
     * @brief Returns all the documents from the collection
     * as a JSON object.
     *
     * @param result All the documents from the collection.
     */
    void all(Json::Value* result) const;

    /**
     * @brief Returns the last record id used by the collection.
     *
     * @return The last record id.
     */
    uint64_t last_record_id() const;

    /**
     * @brief Returns the number of documents stored in the
     * collection.
     *
     * @return The size of the collection.
     */
    size_t size() const;

    /**
     * @brief Erases a document from the collection.
     *
     * @param id Record id of the document to erase.
     */
    void erase(uint64_t id) const;

    private:

    /**
     * @brief Constructor. This constructor is private.
     * Use a Database object to get a Collection instance.
     *
     * @param impl Pointer to implementation.
     */
    Collection(const std::shared_ptr<CollectionImpl>& impl);

    std::shared_ptr<CollectionImpl> self;
};

}

#endif
