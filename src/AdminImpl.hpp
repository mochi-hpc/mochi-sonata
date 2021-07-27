/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_ADMIN_IMPL_H
#define __SONATA_ADMIN_IMPL_H

#include <thallium.hpp>

namespace sonata {

namespace tl = thallium;

class AdminImpl {

public:
  tl::engine m_engine;
  tl::remote_procedure m_create_database;
  tl::remote_procedure m_attach_database;
  tl::remote_procedure m_detach_database;
  tl::remote_procedure m_destroy_database;
  tl::remote_procedure m_list_databases;

  AdminImpl(const tl::engine &engine)
      : m_engine(engine),
        m_create_database(m_engine.define("sonata_create_database")),
        m_attach_database(m_engine.define("sonata_attach_database")),
        m_detach_database(m_engine.define("sonata_detach_database")),
        m_destroy_database(m_engine.define("sonata_destroy_database")),
        m_list_databases(m_engine.define("sonata_list_databases")) {}

  AdminImpl(margo_instance_id mid) : AdminImpl(tl::engine(mid)) {}

  ~AdminImpl() {}
};

} // namespace sonata

#endif
