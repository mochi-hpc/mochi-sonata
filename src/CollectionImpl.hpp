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
  Json::CharReader *m_json_reader = nullptr;

  CollectionImpl() = default;

  CollectionImpl(const std::shared_ptr<DatabaseImpl> &db,
                 const std::string name)
      : m_name(name), m_database(db) {
    Json::CharReaderBuilder builder;
    m_json_reader = builder.newCharReader();
  }

  ~CollectionImpl() { delete m_json_reader; }
};

} // namespace sonata

#endif
