/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "AggregatorBackend.hpp"

namespace sonata {

namespace tl = thallium;
using namespace std::string_literals;

SONATA_REGISTER_BACKEND(aggregator, AggregatorBackend);

std::unique_ptr<Backend> AggregatorBackend::create(const tl::engine &engine,
                                             const tl::pool &pool,
                                             const json &config) {
  spdlog::trace("[aggregator] Creating Cached database");
  std::string backend_type = config.value("backend", "");
  if (backend_type.size() == 0) {
    throw Exception(
        "AggregatorBackend needs to be initialized with a \"backend\" entry");
  }
  bool flush_on_read = config.value("flush_on_read", true);
  bool flush_on_exec = config.value("flush_on_exec", true);
  size_t batch_size  = config.value("batch_size", (size_t)32);
  bool commit_on_flush = config.value("commit_on_flush", true);
  const auto &inner_cfg = config["config"];
  std::unique_ptr<Backend> inner =
      BackendFactory::createBackend(backend_type, engine, pool, inner_cfg);
  auto backend = std::make_unique<AggregatorBackend>(std::move(inner), pool,
                                               flush_on_read,
                                               flush_on_exec,
                                               batch_size,
                                               commit_on_flush);
  spdlog::trace("[aggregator] Successfully created database");
  return backend;
}

std::unique_ptr<Backend> AggregatorBackend::attach(const thallium::engine &engine,
                                             const tl::pool &pool,
                                             const json &config) {
  spdlog::trace("[aggregator] Opening Cached database");
  std::string backend_type = config.value("backend", "");
  if (backend_type.size() == 0) {
    throw Exception(
        "AggregatorBackend needs to be initialized with a \"backend\" entry");
  }
  const auto &inner_cfg = config["config"];
  bool flush_on_read = config.value("flush_on_read", true);
  bool flush_on_exec = config.value("flush_on_exec", true);
  size_t batch_size  = config.value("batch_size", (size_t)32);
  bool commit_on_flush = config.value("commit_on_flush", true);
  std::unique_ptr<Backend> inner =
      BackendFactory::attachBackend(backend_type, engine, pool, inner_cfg);
  auto backend = std::make_unique<AggregatorBackend>(std::move(inner), pool,
                                               flush_on_read,
                                               flush_on_exec,
                                               batch_size,
                                               commit_on_flush);
  spdlog::trace("[aggregator] Successfully opened database");
  return backend;
}

} // namespace sonata
