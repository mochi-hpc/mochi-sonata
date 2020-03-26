#ifndef __SONATA_PROVIDER_IMPL_H
#define __SONATA_PROVIDER_IMPL_H

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>

namespace sonata {

namespace tl = thallium;

class ProviderImpl : public tl::provider<ProviderImpl> {

    public:

    tl::pool             m_pool;
    tl::remote_procedure m_create_database;
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

    ProviderImpl(tl::engine& engine, uint16_t provider_id, const tl::pool& pool)
    : tl::provider<ProviderImpl>(engine, provider_id)
    , m_pool(pool)
    , m_create_database(   define("sonata_create_database",   &ProviderImpl::createDatabase, pool)   )
    , m_open_database(     define("sonata_open_database",     &ProviderImpl::openDatabase)     )
    , m_create_collection( define("sonata_create_collection", &ProviderImpl::createCollection) )
    , m_drop_collection(   define("sonata_drop_collection",   &ProviderImpl::dropCollection)   )
    , m_coll_store(        define("sonata_store",             &ProviderImpl::store)            )
    , m_coll_fetch(        define("sonata_fetch",             &ProviderImpl::fetch)            )
    , m_coll_filter(       define("sonata_filter",            &ProviderImpl::filter)           )
    , m_coll_update(       define("sonata_update",            &ProviderImpl::update)           )
    , m_coll_all(          define("sonata_all",               &ProviderImpl::all)              )
    , m_coll_last_id(      define("sonata_last_id",           &ProviderImpl::lastID)           )
    , m_coll_size(         define("sonata_size",              &ProviderImpl::size)             )
    , m_coll_erase(        define("sonata_erase",             &ProviderImpl::erase)            )
    {}

    void createDatabase(const tl::request& req,
                        const std::string& token,
                        const std::string& db_name,
                        const std::string& db_path) {
        bool ret = true;
        // TODO
        req.respond(ret);
    }

    void openDatabase(const tl::request& req,
                      const std::string& db_name) {
        bool ret = true;
        // TODO
        req.respond(ret);
    }

    void createCollection(const tl::request& req,
                          const std::string& db_name,
                          const std::string& coll_name) {
        bool ret = true;
        // TODO
        req.respond(ret);
    }

    void dropCollection(const tl::request& req,
                        const std::string& db_name,
                        const std::string& coll_name) {
    
        bool ret = true;
        // TODO
        req.respond(ret);    
    }

    void store(const tl::request& req,
               const std::string& db_name,
               const std::string& coll_name,
               const std::string& record) {
        auto ret = std::make_pair<bool,uint64_t>(false,0);
        // TODO
        req.respond(ret);
    }

    void fetch(const tl::request& req,
               const std::string& db_name,
               const std::string& coll_name,
               uint64_t record_id) {
        auto ret = std::make_pair<bool,std::string>(false, "");
        // TODO
        req.respond(ret);
    }

    void filter(const tl::request& req,
                const std::string& db_name,
                const std::string& coll_name,
                const std::string& filter_code) {
        auto ret = std::make_pair<bool,std::string>(false, "");
        // TODO
        req.respond(ret);
    }

    void update(const tl::request& req,
                const std::string& db_name,
                const std::string& coll_name,
                uint64_t record_id,
                const std::string& new_content) {
        bool ret = true;
        // TODO
        req.respond(ret);
    }

    void all(const tl::request& req,
             const std::string& db_name,
             const std::string& coll_name) {
        auto ret = std::make_pair<bool,std::string>(false,"");
        // TODO
        req.respond(ret);
    }

    void lastID(const tl::request& req,
                const std::string& db_name,
                const std::string& coll_name) {
        uint64_t ret = 0;
        // TODO
        req.respond(ret);
    }

    void size(const tl::request& req,
              const std::string& db_name,
              const std::string& coll_name) {
        uint64_t ret = 0;
        // TODO
        req.respond(ret);
    }

    void erase(const tl::request& req,
               const std::string& db_name,
               const std::string& coll_name,
               uint64_t record_id) {
        bool ret = true;
        // TODO
        req.respond(ret);
    }
};

}

#endif
