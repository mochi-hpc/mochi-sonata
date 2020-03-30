#ifndef __SONATA_CLIENT_IMPL_H
#define __SONATA_CLIENT_IMPL_H

#include <thallium.hpp>

namespace sonata {

namespace tl = thallium;

class ClientImpl {

    public:

    tl::engine&          m_engine;
    tl::remote_procedure m_open_database;
    tl::remote_procedure m_create_collection;
    tl::remote_procedure m_open_collection;
    tl::remote_procedure m_drop_collection;
    tl::remote_procedure m_coll_store;
    tl::remote_procedure m_coll_fetch;
    tl::remote_procedure m_coll_filter;
    tl::remote_procedure m_coll_update;
    tl::remote_procedure m_coll_all;
    tl::remote_procedure m_coll_last_id;
    tl::remote_procedure m_coll_size;
    tl::remote_procedure m_coll_erase;

    ClientImpl(tl::engine& engine)
    : m_engine(engine)
    , m_open_database(     engine.define("sonata_open_database")     )
    , m_create_collection( engine.define("sonata_create_collection") )
    , m_open_collection(   engine.define("sonata_open_collection")   )
    , m_drop_collection(   engine.define("sonata_drop_collection")   )
    , m_coll_store(        engine.define("sonata_store")             )
    , m_coll_fetch(        engine.define("sonata_fetch")             )
    , m_coll_filter(       engine.define("sonata_filter")            )
    , m_coll_update(       engine.define("sonata_update")            )
    , m_coll_all(          engine.define("sonata_all")               )
    , m_coll_last_id(      engine.define("sonata_last_id")           )
    , m_coll_size(         engine.define("sonata_size")              )
    , m_coll_erase(        engine.define("sonata_erase")             )
    {}

};

}

#endif
