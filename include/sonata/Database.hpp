#ifndef __SONATA_DATABASE_HPP
#define __SONATA_DATABASE_HPP

#include <thallium.hpp>
#include <memory>
#include <sonata/Collection.hpp>

namespace sonata {

namespace tl = thallium;

class Client;
class DatabaseImpl;

class Database {

    friend class Client;

    public:

    Database(const Database&);

    Database(Database&&);

    Database& operator=(const Database&);

    Database& operator=(Database&&);

    ~Database();

    Collection create(const std::string& collectionName) const;

    bool drop(const std::string& collectionName) const;

    operator bool() const;

    private:

    Database(const std::shared_ptr<DatabaseImpl>& impl);

    std::shared_ptr<DatabaseImpl> self;
};

}

#endif
