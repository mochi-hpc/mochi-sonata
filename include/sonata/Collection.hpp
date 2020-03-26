#ifndef __SONATA_COLLECTION_HPP
#define __SONATA_COLLECTION_HPP

#include <json/json.h>
#include <thallium.hpp>
#include <memory>

namespace sonata {

namespace tl = thallium;

class DatabaseImpl;
class CollectionImpl;

class Collection {

    friend class Database;

    public:
    
    Collection(const Collection&);

    Collection(Collection&&);

    Collection& operator=(const Collection&);

    Collection& operator=(Collection&&);

    ~Collection();

    operator bool() const;

    bool store(const std::string& record,
               uint64_t* id) const;

    bool store(const Json::Value& record,
               uint64_t* id) const;

    bool fetch(uint64_t id,
               std::string* result) const;

    bool fetch(uint64_t id,
               Json::Value* result) const;

    bool filter(const std::string& filterCode,
                std::string* result) const;

    bool filter(const std::string& filterCode,
                Json::Value* result) const;

    bool update(uint64_t id,
                const Json::Value& record) const;

    bool update(uint64_t id,
                const std::string& record) const;

    bool all(std::string* result) const;

    bool all(Json::Value* result) const;

    uint64_t last_record_id() const;

    size_t size() const;

    bool erase(uint64_t id) const;

    private:

    Collection(const std::shared_ptr<CollectionImpl>& impl);

    std::shared_ptr<CollectionImpl> self;
};

}

#endif
