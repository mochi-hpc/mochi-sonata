#ifndef __SONATA_ADMIN_IMPL_H
#define __SONATA_ADMIN_IMPL_H

#include <thallium.hpp>

namespace sonata {

namespace tl = thallium;

class AdminImpl {

    public:

    std::string          m_token;
    tl::engine&          m_engine;
    tl::remote_procedure m_create_database;
    tl::remote_procedure m_attach_database;
    tl::remote_procedure m_detach_database;
    tl::remote_procedure m_destroy_database;

    AdminImpl(tl::engine& engine, const std::string& token)
    : m_token(token)
    , m_engine(engine)
    , m_create_database(engine.define("sonata_create_database"))
    , m_attach_database(engine.define("sonata_attach_database"))
    , m_detach_database(engine.define("sonata_detach_database"))
    , m_destroy_database(engine.define("sonata_destroy_database"))
    {}

};

}

#endif
