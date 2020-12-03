/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "NullBackend.hpp"

namespace sonata {

namespace tl = thallium;
using namespace std::string_literals;

SONATA_REGISTER_BACKEND(null, NullBackend);

std::unique_ptr<Backend> NullBackend::create(const tl::engine &engine,
                                             const tl::pool &pool,
                                             const Json::Value &config) {
  spdlog::trace("[null] Creating Null database");
  auto backend = std::make_unique<NullBackend>();
  spdlog::trace("[null] Successfully created database");
  return backend;
}

std::unique_ptr<Backend> NullBackend::attach(const tl::engine &engine,
                                             const tl::pool &pool,
                                             const Json::Value &config) {
  spdlog::trace("[null] Opening Null database");
  auto backend = std::make_unique<NullBackend>();
  spdlog::trace("[null] Successfully opened database");
  return backend;
}

} // namespace sonata
