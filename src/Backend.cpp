/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "sonata/Backend.hpp"

namespace tl = thallium;
using nlohmann::json;

namespace sonata {

std::unordered_map<std::string, std::function<std::unique_ptr<Backend>(
                                    const tl::engine &, const tl::pool &,
                                    const json &)>>
    BackendFactory::create_fn;

std::unordered_map<std::string, std::function<std::unique_ptr<Backend>(
                                    const tl::engine &, const tl::pool &,
                                    const json &)>>
    BackendFactory::attach_fn;

std::unique_ptr<Backend>
BackendFactory::createBackend(const std::string &backend_name,
                              const tl::engine &engine, const tl::pool &pool,
                              const json &config) {
  auto it = create_fn.find(backend_name);
  if (it == create_fn.end())
    return nullptr;
  auto &f = it->second;
  return f(engine, pool, config);
}

std::unique_ptr<Backend>
BackendFactory::attachBackend(const std::string &backend_name,
                              const tl::engine &engine, const tl::pool &pool,
                              const json &config) {
  auto it = attach_fn.find(backend_name);
  if (it == attach_fn.end())
    return nullptr;
  auto &f = it->second;
  return f(engine, pool, config);
}

} // namespace sonata
