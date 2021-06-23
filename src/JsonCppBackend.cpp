/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "JsonCppBackend.hpp"

namespace sonata {

namespace tl = thallium;
using namespace std::string_literals;
using nlohmann::json;

SONATA_REGISTER_BACKEND(jsoncpp, JsonCppBackend);

std::unique_ptr<Backend> JsonCppBackend::create(const thallium::engine &engine,
                                                const tl::pool &pool,
                                                const json &config) {
  spdlog::trace("[jsoncpp] Creating JsonCpp database");
  int ret;
  auto backend = std::make_unique<JsonCppBackend>();
  spdlog::trace("[jsoncpp] Successfully created database");
  return backend;
}

std::unique_ptr<Backend> JsonCppBackend::attach(const thallium::engine &engine,
                                                const tl::pool &pool,
                                                const json &config) {
  spdlog::trace("[jsoncpp] Opening JsonCpp database");
  int ret;
  auto backend = std::make_unique<JsonCppBackend>();
  spdlog::trace("[jsoncpp] Successfully opened database");
  return backend;
}

} // namespace sonata
