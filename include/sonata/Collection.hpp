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

    uint64_t store(const std::string& record) const;

    uint64_t store(const Json::Value& record) const;

    void fetch(uint64_t id,
               std::string* result) const;

    void fetch(uint64_t id,
               Json::Value* result) const;

    void filter(const std::string& filterCode,
                std::string* result) const;

    void filter(const std::string& filterCode,
                Json::Value* result) const;

    void update(uint64_t id,
                const Json::Value& record) const;

    void update(uint64_t id,
                const std::string& record) const;

    void all(std::string* result) const;

    void all(Json::Value* result) const;

    uint64_t last_record_id() const;

    size_t size() const;

    void erase(uint64_t id) const;

    private:

    Collection(const std::shared_ptr<CollectionImpl>& impl);

    std::shared_ptr<CollectionImpl> self;
};

}

#endif
