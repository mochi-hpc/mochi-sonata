/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_CLIENT_IMPL_H
#define __SONATA_CLIENT_IMPL_H

#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/unordered_map.hpp>
#include <thallium/serialization/stl/unordered_set.hpp>

namespace sonata {

namespace tl = thallium;

class ClientImpl {

public:
  tl::engine m_engine;
  tl::remote_procedure m_open_database;
  tl::remote_procedure m_create_collection;
  tl::remote_procedure m_open_collection;
  tl::remote_procedure m_drop_collection;
  tl::remote_procedure m_execute_on_database;
  tl::remote_procedure m_commit;
  tl::remote_procedure m_coll_store;
  tl::remote_procedure m_coll_store_json;
  tl::remote_procedure m_coll_store_multi;
  tl::remote_procedure m_coll_store_multi_json;
  tl::remote_procedure m_coll_fetch;
  tl::remote_procedure m_coll_fetch_json;
  tl::remote_procedure m_coll_fetch_multi;
  tl::remote_procedure m_coll_fetch_multi_json;
  tl::remote_procedure m_coll_filter;
  tl::remote_procedure m_coll_filter_json;
  tl::remote_procedure m_coll_update;
  tl::remote_procedure m_coll_update_json;
  tl::remote_procedure m_coll_update_multi;
  tl::remote_procedure m_coll_update_multi_json;
  tl::remote_procedure m_coll_all;
  tl::remote_procedure m_coll_all_json;
  tl::remote_procedure m_coll_last_id;
  tl::remote_procedure m_coll_size;
  tl::remote_procedure m_coll_erase;
  tl::remote_procedure m_coll_erase_multi;

  ClientImpl(const tl::engine &engine)
      : m_engine(engine),
        m_open_database(m_engine.define("sonata_open_database")),
        m_create_collection(m_engine.define("sonata_create_collection")),
        m_open_collection(m_engine.define("sonata_open_collection")),
        m_drop_collection(m_engine.define("sonata_drop_collection")),
        m_execute_on_database(m_engine.define("sonata_exec_on_database")),
        m_commit(m_engine.define("sonata_commit")),
        m_coll_store(m_engine.define("sonata_store")),
        m_coll_store_json(m_engine.define("sonata_store_json")),
        m_coll_store_multi(m_engine.define("sonata_store_multi")),
        m_coll_store_multi_json(m_engine.define("sonata_store_multi_json")),
        m_coll_fetch(m_engine.define("sonata_fetch")),
        m_coll_fetch_json(m_engine.define("sonata_fetch_json")),
        m_coll_fetch_multi(m_engine.define("sonata_fetch_multi")),
        m_coll_fetch_multi_json(m_engine.define("sonata_fetch_multi_json")),
        m_coll_filter(m_engine.define("sonata_filter")),
        m_coll_filter_json(m_engine.define("sonata_filter_json")),
        m_coll_update(m_engine.define("sonata_update")),
        m_coll_update_json(m_engine.define("sonata_update_json")),
        m_coll_update_multi(m_engine.define("sonata_update_multi")),
        m_coll_update_multi_json(m_engine.define("sonata_update_multi_json")),
        m_coll_all(m_engine.define("sonata_all")),
        m_coll_all_json(m_engine.define("sonata_all_json")),
        m_coll_last_id(m_engine.define("sonata_last_id")),
        m_coll_size(m_engine.define("sonata_size")),
        m_coll_erase(m_engine.define("sonata_erase")),
        m_coll_erase_multi(m_engine.define("sonata_erase_multi")) {}

  ClientImpl(margo_instance_id mid) : ClientImpl(tl::engine(mid)) {}

  ~ClientImpl() {}
};

} // namespace sonata

#endif
