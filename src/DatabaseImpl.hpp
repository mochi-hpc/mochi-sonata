/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_DATABASE_IMPL_H
#define __SONATA_DATABASE_IMPL_H

namespace sonata {

namespace tl = thallium;

class DatabaseImpl {

public:
  std::string m_name;
  std::shared_ptr<ClientImpl> m_client;
  tl::provider_handle m_ph;

  DatabaseImpl() = default;

  DatabaseImpl(const std::shared_ptr<ClientImpl> &client,
               const tl::provider_handle &ph, const std::string &name)
      : m_name(name), m_client(client), m_ph(ph) {}

  ~DatabaseImpl() = default;
};

} // namespace sonata

#endif
