/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_COLLECTION_IMPL_H
#define __SONATA_COLLECTION_IMPL_H

namespace sonata {

class DatabaseImpl;

class CollectionImpl {

public:
  std::string m_name;
  std::shared_ptr<DatabaseImpl> m_database;

  CollectionImpl() = default;

  CollectionImpl(const std::shared_ptr<DatabaseImpl> &db,
                 const std::string name)
      : m_name(name), m_database(db) {}

  ~CollectionImpl() = default;
};

} // namespace sonata

#endif
