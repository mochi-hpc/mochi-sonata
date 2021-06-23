/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "VectorBackend.hpp"

namespace sonata {

namespace tl = thallium;
using namespace std::string_literals;

SONATA_REGISTER_BACKEND(vector, VectorBackend);

std::unique_ptr<Backend> VectorBackend::create(const thallium::engine &engine,
                                                const tl::pool &pool,
                                                const json &config) {
  spdlog::trace("[vector] Creating Vector database");
  int ret;
  auto backend = std::make_unique<VectorBackend>();
  spdlog::trace("[vector] Successfully created database");
  return backend;
}

std::unique_ptr<Backend> VectorBackend::attach(const thallium::engine &engine,
                                                const tl::pool &pool,
                                                const json &config) {
  spdlog::trace("[vector] Opening Vector database");
  int ret;
  auto backend = std::make_unique<VectorBackend>();
  spdlog::trace("[vector] Successfully opened database");
  return backend;
}

} // namespace sonata
