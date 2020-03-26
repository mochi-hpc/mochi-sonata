#ifndef __SONATA_PROVIDER_IMPL_H
#define __SONATA_PROVIDER_IMPL_H

#include "sonata/Backend.hpp"

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>

#include <json/json.h>

#include <tuple>

namespace sonata {

namespace tl = thallium;
using namespace std::string_literals;

class ProviderImpl : public tl::provider<ProviderImpl> {

    public:

    tl::pool             m_pool;
    // Admin RPC
    tl::remote_procedure m_attach_database;
    tl::remote_procedure m_detach_database;
    tl::remote_procedure m_destroy_database;
    // Client RPC
    tl::remote_procedure m_open_database;
    tl::remote_procedure m_create_collection;
    tl::remote_procedure m_drop_collection;
    tl::remote_procedure m_coll_store;
    tl::remote_procedure m_coll_fetch;
    tl::remote_procedure m_coll_filter;
    tl::remote_procedure m_coll_update;
    tl::remote_procedure m_coll_all;
    tl::remote_procedure m_coll_last_id;
    tl::remote_procedure m_coll_size;
    tl::remote_procedure m_coll_erase;
    // Backends
    std::unordered_map<std::string, std::unique_ptr<Backend>> m_backends;

    ProviderImpl(tl::engine& engine, uint16_t provider_id, const tl::pool& pool)
    : tl::provider<ProviderImpl>(engine, provider_id)
    , m_pool(pool)
    , m_attach_database(   define("sonata_attach_database",   &ProviderImpl::attachDatabase,   pool))
    , m_detach_database(   define("sonata_detach_database",   &ProviderImpl::detachDatabase,   pool))
    , m_destroy_database(  define("sonata_destroy_database",  &ProviderImpl::destroyDatabase,  pool))
    , m_open_database(     define("sonata_open_database",     &ProviderImpl::openDatabase,     pool))
    , m_create_collection( define("sonata_create_collection", &ProviderImpl::createCollection, pool))
    , m_drop_collection(   define("sonata_drop_collection",   &ProviderImpl::dropCollection,   pool))
    , m_coll_store(        define("sonata_store",             &ProviderImpl::store,            pool))
    , m_coll_fetch(        define("sonata_fetch",             &ProviderImpl::fetch,            pool))
    , m_coll_filter(       define("sonata_filter",            &ProviderImpl::filter,           pool))
    , m_coll_update(       define("sonata_update",            &ProviderImpl::update,           pool))
    , m_coll_all(          define("sonata_all",               &ProviderImpl::all,              pool))
    , m_coll_last_id(      define("sonata_last_id",           &ProviderImpl::lastID,           pool))
    , m_coll_size(         define("sonata_size",              &ProviderImpl::size,             pool))
    , m_coll_erase(        define("sonata_erase",             &ProviderImpl::erase,            pool))
    {}

    ~ProviderImpl() {
        m_attach_database.deregister();
        m_detach_database.deregister();
        m_destroy_database.deregister();
        m_open_database.deregister();
        m_create_collection.deregister();
        m_drop_collection.deregister();
        m_coll_store.deregister();
        m_coll_fetch.deregister();
        m_coll_filter.deregister();
        m_coll_update.deregister();
        m_coll_all.deregister();
        m_coll_last_id.deregister();
        m_coll_size.deregister();
        m_coll_erase.deregister();
    }

    void attachDatabase(const tl::request& req,
                        const std::string& token,
                        const std::string& db_name,
                        const std::string& db_type,
                        const std::string& db_config) {

        auto ret = std::make_pair<bool,std::string>(true,"");

        if(m_backends.count(db_name) != 0) {
            ret.first = false;
            ret.second = "Backend "s + db_name + " already exists";
            req.respond(ret);
            return;
        }

        Json::CharReaderBuilder builder;
        Json::CharReader* reader = builder.newCharReader();

        Json::Value json_config;
        std::string errors;

        bool parsingSuccessful = reader->parse(
                db_config.c_str(),
                db_config.c_str() + db_config.size(),
                &json_config,
                &errors
                );
        delete reader;

        if(!parsingSuccessful) {
            ret.first = false;
            ret.second = std::move(errors);
            req.respond(ret);
            return;
        }

        auto backend = BackendFactory::createBackend(db_type, json_config);
        if(not backend) {
            ret.first = false;
            ret.second = "Unknown backend type "s + db_type;
        } else {
            m_backends[db_name] = std::move(backend);
        }

        req.respond(ret);
    }

    void detachDatabase(const tl::request& req,
                        const std::string& token,
                        const std::string& db_name) {
        auto ret = std::make_pair<bool,std::string>(true,"");
        if(m_backends.count(db_name) == 0) {
            ret.first = false;
            ret.second = "Backend "s + db_name + " not found";
            req.respond(ret);
            return;
        }

        m_backends.erase(db_name);
        req.respond(ret);
    }
    
    void destroyDatabase(const tl::request& req,
                         const std::string& token,
                         const std::string& db_name) {
        RequestResult<bool> result;
        if(m_backends.count(db_name) == 0) {
            result.success() = false;
            result.error() = "Backend "s + db_name + " not found";
            req.respond(result);
            return;
        }

        result = m_backends[db_name]->destroy();
        m_backends.erase(db_name);

        req.respond(result);
    }

    void openDatabase(const tl::request& req,
                      const std::string& db_name) {
        auto it = m_backends.find(db_name);
        RequestResult<bool> result;
        if(it == m_backends.end()) {
            result.success() = false;
            result.error() = "Backend "s + db_name + " not found";
            req.respond(result);
            return;
        }
        req.respond(result);
    }

    void createCollection(const tl::request& req,
                          const std::string& db_name,
                          const std::string& coll_name) {
        RequestResult<bool> result;
        auto it = m_backends.find(db_name);
        if(it == m_backends.end()) {
            result.success() = false;
            result.error() = "Backend "s + db_name + " not found";
            req.respond(result);
            return;
        }
        result = it->second->createCollection(coll_name);
        req.respond(result);
    }

    void dropCollection(const tl::request& req,
                        const std::string& db_name,
                        const std::string& coll_name) {
        RequestResult<bool> result;
        auto it = m_backends.find(db_name);
        if(it == m_backends.end()) {
            result.success() = false;
            result.error() = "Backend "s + db_name + " not found";
            req.respond(result);
            return;
        }
        result = it->second->dropCollection(coll_name);
        req.respond(result);    
    }

    void store(const tl::request& req,
               const std::string& db_name,
               const std::string& coll_name,
               const std::string& record) {
        RequestResult<uint64_t> result;
        auto it = m_backends.find(db_name);
        if(it == m_backends.end()) {
            result.success() = false;
            result.error() = "Backend "s + db_name + " not found";
            req.respond(result);
            return;
        }
        result = it->second->store(coll_name, record);
        req.respond(result);
    }

    void fetch(const tl::request& req,
               const std::string& db_name,
               const std::string& coll_name,
               uint64_t record_id) {
        RequestResult<std::string> result;
        auto it = m_backends.find(db_name);
        if(it == m_backends.end()) {
            result.success() = false;
            result.error() = "Backend "s + db_name + " not found";
            req.respond(result);
            return;
        }
        result = it->second->fetch(coll_name, record_id);
        req.respond(result);
    }

    void filter(const tl::request& req,
                const std::string& db_name,
                const std::string& coll_name,
                const std::string& filter_code) {
        RequestResult<std::string> result;
        auto it = m_backends.find(db_name);
        if(it == m_backends.end()) {
            result.success() = false;
            result.error() = "Backend "s + db_name + " not found";
            req.respond(result);
            return;
        }
        result = it->second->filter(coll_name, filter_code);
        req.respond(result);
    }

    void update(const tl::request& req,
                const std::string& db_name,
                const std::string& coll_name,
                uint64_t record_id,
                const std::string& new_content) {
        RequestResult<bool> result;
        auto it = m_backends.find(db_name);
        if(it == m_backends.end()) {
            result.success() = false;
            result.error() = "Backend "s + db_name + " not found";
            req.respond(result);
            return;
        }
        result = it->second->update(coll_name, record_id, new_content);
        req.respond(result);
    }

    void all(const tl::request& req,
             const std::string& db_name,
             const std::string& coll_name) {
        RequestResult<std::string> result;
        auto it = m_backends.find(db_name);
        if(it == m_backends.end()) {
            result.success() = false;
            result.error() = "Backend "s + db_name + " not found";
            req.respond(result);
            return;
        }
        result = it->second->all(coll_name);
        req.respond(result);
    }

    void lastID(const tl::request& req,
                const std::string& db_name,
                const std::string& coll_name) {
        RequestResult<uint64_t> result;
        auto it = m_backends.find(db_name);
        if(it == m_backends.end()) {
            result.success() = false;
            result.error() = "Backend "s + db_name + " not found";
            req.respond(result);
            return;
        }
        result = it->second->lastID(coll_name); 
        req.respond(result);
    }

    void size(const tl::request& req,
              const std::string& db_name,
              const std::string& coll_name) {
        RequestResult<uint64_t> result;
        auto it = m_backends.find(db_name);
        if(it == m_backends.end()) {
            result.success() = false;
            result.error() = "Backend "s + db_name + " not found";
            req.respond(result);
            return;
        }
        result = it->second->size(coll_name);
        req.respond(result);
    }

    void erase(const tl::request& req,
               const std::string& db_name,
               const std::string& coll_name,
               uint64_t record_id) {
        RequestResult<bool> result;
        auto it = m_backends.find(db_name);
        if(it == m_backends.end()) {
            result.success() = false;
            result.error() = "Backend "s + db_name + " not found";
            req.respond(result);
            return;
        }
        result = it->second->erase(coll_name, record_id);
        req.respond(result);
    }
};

}

#endif
