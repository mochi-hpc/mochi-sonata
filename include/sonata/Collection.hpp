/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_COLLECTION_HPP
#define __SONATA_COLLECTION_HPP

#include <json/json.h>
#include <memory>
#include <sonata/AsyncRequest.hpp>
#include <sonata/Database.hpp>
#include <thallium.hpp>

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
  Collection(const Collection &);

  /**
   * @brief Move constructor.
   */
  Collection(Collection &&);

  /**
   * @brief Copy-assignment operator.
   */
  Collection &operator=(const Collection &);

  /**
   * @brief Move-assignment operator.
   */
  Collection &operator=(Collection &&);

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
   * @param commit Whether to commit the change to storage.
   *
   * @return the record id of the stored document.
   */
  inline uint64_t store(const std::string &record, bool commit = false) const {
    uint64_t record_id;
    store(record, &record_id, commit);
    return record_id;
  }

  /**
   * @brief Stores a document into the collection.
   *
   * @param record A JSON object.
   * @param commit Whether to commit the change to storage.
   *
   * @return the record id of the stored document.
   */
  inline uint64_t store(const Json::Value &record, bool commit = false) const {
    uint64_t record_id;
    store(record, &record_id, commit);
    return record_id;
  }

  /**
   * @brief Stores a document into the collection.
   *
   * @param record A valid JSON-formated document.
   * @param commit Whether to commit the change to storage.
   *
   * @return the record id of the stored document.
   */
  inline uint64_t store(const char *record, bool commit = false) const {
    uint64_t record_id;
    store(record, &record_id, commit);
    return record_id;
  }

  /**
   * @brief Asynchronously stores a document into the collection.
   * If the pointer to the request is null, this function will be
   * executed synchronously.
   *
   * @param record A valid JSON-formated document.
   * @param id Pointer to a record id set when the request completes.
   * @param commit Whether to commit the changes to storage.
   * @param req Pointer to a request to wait on.
   *
   * @return the record id of the stored document.
   */
  void store(const std::string &record, uint64_t *id, bool commit = false,
             AsyncRequest *req = nullptr) const;

  /**
   * @brief Asynchronously stores a document into the collection.
   * If the pointer to the request is null, this function will be
   * executed synchronously.
   *
   * @param record A JSON document.
   * @param id Pointer to a record id set when the request completes.
   * @param commit Whether to commit the changes to storage.
   * @param req Pointer to a request to wait on.
   *
   * @return the record id of the stored document.
   */
  void store(const Json::Value &record, uint64_t *id, bool commit = false,
             AsyncRequest *req = nullptr) const;

  /**
   * @brief Asynchronously stores a document into the collection.
   * If the pointer to the request is null, this function will be
   * executed synchronously.
   *
   * @param record A valid JSON-formated document.
   * @param id Pointer to a record id set when the request completes.
   * @param req Pointer to a request to wait on.
   *
   * @return the record id of the stored document.
   */
  void store(const char *record, uint64_t *id, bool commit = false,
             AsyncRequest *req = nullptr) const {
    store(std::string(record), id, commit, req);
  }

  /**
   * @brief Stores multiple records in the collection. Each string
   * in the vector should be a valid JSON object. The ids parameter
   * should be a pointer to a memory region sufficiently large to
   * store the resulting records.size() identifiers.
   *
   * This function will either store all or none of the records.
   *
   * @param records Vector of records to store.
   * @param ids Resulting ids.
   * @param commit Whether to commit the changes to storage.
   * @param req Pointer to a request to wait on.
   */
  void store_multi(const std::vector<std::string> &records, uint64_t *ids,
                   bool commit = false, AsyncRequest *req = nullptr) const;

  /**
   * @brief Stores multiple records in the collection. The provided
   * Json::Value should be an array of objects. The ids parameter
   * should be a pointer to a memory region sufficiently large to
   * store the resulting records.size() identifiers.
   *
   * This function will either store all or none of the records.
   *
   * @param records JSON array of records to store.
   * @param ids Resulting ids.
   * @param commit Whether to commit the changes to storage.
   * @param req Pointer to a request to wait on.
   */
  void store_multi(const Json::Value &records, uint64_t *ids,
                   bool commit = false, AsyncRequest *req = nullptr) const;

  /**
   * @brief Stores multiple records in the collection. Each null-terminated
   * string in the array should be a valid JSON object. The ids parameter
   * should be a pointer to a memory region sufficiently large to store the
   * resulting count identifiers.
   *
   * This function will either store all or none of the records.
   *
   * @param records Vector of records to store.
   * @param count Number of records to store.
   * @param ids Resulting ids.
   * @param commit Whether to commit the changes to storage.
   * @param req Pointer to a request to wait on.
   */
  void store_multi(const char *const *records, size_t count, uint64_t *ids,
                   bool commit = false, AsyncRequest *req = nullptr) const {
    std::vector<std::string> vec;
    vec.reserve(count);
    for (size_t i = 0; i < count; i++) {
      vec.emplace_back(records[i]);
    }
    store_multi(vec, ids, commit, req);
  }

  /**
   * @brief Asynchronously fetches a document by its record id.
   * If req is null, this function becomes synchronous.
   *
   * @param[in] id Record id.
   * @param[out] result Resulting string.
   * @param req Pointer to a request to wait on.
   */
  void fetch(uint64_t id, std::string *result,
             AsyncRequest *req = nullptr) const;

  /**
   * @brief Asynchronously fetches a document by its record id.
   * If req is null, this function becomes synchronous.
   *
   * @param[in] id Record id.
   * @param[out] result Resulting JSON object.
   * @param req Pointer to a request to wait on.
   */
  void fetch(uint64_t id, Json::Value *result,
             AsyncRequest *req = nullptr) const;

  /**
   * @brief Asynchronously fetches multiple documents by their record id.
   * If req is null, this function becomes synchronous.
   *
   * @param[in] ids Array of record ids.
   * @param[in] count Number of records.
   * @param[out] result Resulting string.
   * @param req Pointer to a request to wait on.
   */
  void fetch_multi(const uint64_t *ids, size_t count,
                   std::vector<std::string> *result,
                   AsyncRequest *req = nullptr) const;

  /**
   * @brief Asynchronously fetches a document by its record id.
   * If req is null, this function becomes synchronous.
   *
   * @param[in] ids Record ids.
   * @param[in] count Number of records to fetch.
   * @param[out] result Resulting JSON array.
   * @param req Pointer to a request to wait on.
   */
  void fetch_multi(const uint64_t *id, size_t count, Json::Value *result,
                   AsyncRequest *req = nullptr) const;

  /**
   * @brief Asynchronously filters the collection and returns the
   * records that match the condition. This condition should
   * be expressed as a Jx9 function returning TRUE or FALSE.
   * For example the following Jx9 function selects only
   * the records where x < 4 :
   *
   * "function($record) { return $record.x < 4; }"
   *
   * If req is null, this function becomes synchronous.
   *
   * @param filterCode A Jx9 filter code.
   * @param result Resuling vector of records as strings.
   * @param req Pointer to a request to wait on.
   */
  void filter(const std::string &filterCode, std::vector<std::string> *result,
              AsyncRequest *req = nullptr) const;

  /**
   * @brief Asynchronously filters the collection and returns the
   * records that match the condition. This condition should
   * be expressed as a Jx9 function returning TRUE or FALSE.
   * For example the following Jx9 function selects only
   * the records where x < 4 :
   *
   * "function($record) { return $record.x < 4; }"
   *
   * If req is null, this function becomes synchronous.
   *
   * @param filterCode A Jx9 filter code.
   * @param result Resuling JSON object containing the array of results.
   * @param req Pointer to a request to wait on.
   */
  void filter(const std::string &filterCode, Json::Value *result,
              AsyncRequest *req = nullptr) const;

  /**
   * @brief Asynchronously updates the content of a document with a new content.
   * If req is null, this function becomes synchronous.
   *
   * @param id Record id of the document to update.
   * @param record New document.
   * @param commit Whether to commit the changes to storage.
   * @param req Pointer to a request to wait on.
   */
  void update(uint64_t id, const Json::Value &record, bool commit = false,
              AsyncRequest *req = nullptr) const;

  /**
   * @brief Asynchronously updates the content of a document with a new content.
   * If req is null, this function becomes synchronous.
   *
   * @param id Record id of the document to update.
   * @param record New document.
   * @param commit Whether to commit the changes to storage.
   * @param req Pointer to a request to wait on.
   */
  void update(uint64_t id, const std::string &record, bool commit = false,
              AsyncRequest *req = nullptr) const;

  /**
   * @brief Asynchronously updates the content of a document with a new content.
   * If req is null, this function becomes synchronous.
   *
   * @param id Record id of the document to update.
   * @param record New document.
   * @param commit Whether to commit the changes to storage.
   * @param req Pointer to a request to wait on.
   */
  void update(uint64_t id, const char *record, bool commit = false,
              AsyncRequest *req = nullptr) const {
    return update(id, std::string(record));
  }

  /**
   * @brief Asynchronously updates the content of multipl documents with
   * new contents. If req is null, this function becomes synchronous.
   * The provided JSON value should be an array.
   *
   * @param ids Record ids of the documents to update.
   * @param records New documents.
   * @param updated Pointer to a vector that will contain whether
   *                each record was updated.
   * @param commit Whether to commit the changes to storage.
   * @param req Pointer to a request to wait on.
   */
  void update_multi(const uint64_t *id, const Json::Value &record,
                    std::vector<bool> *updated, bool commit = false,
                    AsyncRequest *req = nullptr) const;

  /**
   * @brief Asynchronously updates the content of multiple documents with
   * new contents. If req is null, this function becomes synchronous.
   *
   * @param ids Record ids of the documents to update.
   * @param record New document.
   * @param updated Pointer to a vector that will contain whether
   *                each record was updated.
   * @param commit Whether to commit the changes to storage.
   * @param req Pointer to a request to wait on.
   */
  void update_multi(const uint64_t *ids,
                    const std::vector<std::string> &records,
                    std::vector<bool> *updated, bool commit = false,
                    AsyncRequest *req = nullptr) const;

  /**
   * @brief Asynchronously updates the content of multiple documents with
   * new contents. If req is null, this function becomes synchronous.
   *
   * @param ids Record ids of the documents to update.
   * @param records New documents (as C array of null-terminated strings).
   * @param count Number of documents to update.
   * @param updated Pointer to a vector that will contain whether
   *                each record was updated.
   * @param commit Whether to commit the changes to storage.
   * @param req Pointer to a request to wait on.
   */
  void update_multi(uint64_t *ids, const char *const *records, size_t count,
                    std::vector<bool> *updated, bool commit = false,
                    AsyncRequest *req = nullptr) const {
    std::vector<std::string> vec;
    vec.reserve(count);
    for (size_t i = 0; i < count; i++) {
      vec.emplace_back(records[i]);
    }
    return update_multi(ids, vec, updated, commit, req);
  }

  /**
   * @brief Asynchronously returns all the documents from the collection
   * as a vector of strings.
   * If req is null, this function becomes synchronous.
   *
   * @param result All the documents from the collection.
   * @param req Pointer to a request to wait on.
   */
  void all(std::vector<std::string> *result, AsyncRequest *req = nullptr) const;

  /**
   * @brief Asynchronously returns all the documents from the collection
   * as a JSON object.
   * If req is null, this function becomes synchronous.
   *
   * @param result All the documents from the collection.
   * @param req Pointer to a request to wait on.
   */
  void all(Json::Value *result, AsyncRequest *req = nullptr) const;

  /**
   * @brief Returns the last record id used by the collection.
   *
   * @return The last record id.
   * @param req Pointer to a request to wait on.
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
   * @brief Asynchronously erases a document from the collection.
   * If req is null, this function becomes synchronous.
   *
   * @param id Record id of the document to erase.
   * @param commit Whether to commit the changes to storage.
   * @param req Pointer to a request to wait on.
   */
  void erase(uint64_t id, bool commit = false,
             AsyncRequest *req = nullptr) const;

  /**
   * @brief Asynchronously erases multiple documents from the collection.
   * If req is null, this function becomes synchronous.
   *
   * @param ids Record ids of the documents to erase.
   * @param commit Whether to commit the changes to storage.
   * @param req Pointer to a request to wait on.
   */
  void erase_multi(const uint64_t *ids, size_t size, bool commit = false,
                   AsyncRequest *req = nullptr) const;

private:
  /**
   * @brief Constructor. This constructor is private.
   * Use a Database object to get a Collection instance.
   *
   * @param impl Pointer to implementation.
   */
  Collection(const std::shared_ptr<CollectionImpl> &impl);

  std::shared_ptr<CollectionImpl> self;
};

} // namespace sonata

#endif
