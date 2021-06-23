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
                                             const json &config) {
  spdlog::trace("[null] Creating Null database");
  uint64_t delay_ms = 0;
  bool active_delay = false;
  bool lock_mutex = false;
  if(config.contains("delay_ms")) {
    if(!config["delay_ms"].is_number_unsigned())
        spdlog::error("[null] delay_ms in JSON config should be an unsigned int");
    else
        delay_ms = config["delay_ms"].get<uint64_t>();
  }
  if(config.contains("active_delay")) {
    if(!config["active_delay"].is_boolean())
        spdlog::error("[null] active_delay in JSON config should be a boolean");
    else
        active_delay = config["active_delay"].get<bool>();
  }
  if(config.contains("lock_mutex")) {
    if(!config["lock_mutex"].is_boolean())
        spdlog::error("[null] lock_mutex in JSON config should be a boolean");
    else
        lock_mutex = config["lock_mutex"].get<bool>();
  }
  auto backend = std::make_unique<NullBackend>(engine, delay_ms, active_delay, lock_mutex);
  spdlog::trace("[null] Successfully created database");
  return backend;
}

std::unique_ptr<Backend> NullBackend::attach(const tl::engine &engine,
                                             const tl::pool &pool,
                                             const json &config) {
  spdlog::trace("[null] Opening Null database");
  uint64_t delay_ms = 0;
  bool active_delay = false;
  bool lock_mutex = false;
  if(config.contains("delay_ms")) {
    if(!config["delay_ms"].is_number_unsigned())
        spdlog::error("[null] delay_ms in JSON config should be an unsigned int");
    else
        delay_ms = config["delay_ms"].get<uint64_t>();
  }
  if(config.contains("active_delay")) {
    if(!config["active_delay"].is_boolean())
        spdlog::error("[null] active_delay in JSON config should be a boolean");
    else
        active_delay = config["active_delay"].get<bool>();
  }
  if(config.contains("lock_mutex")) {
    if(!config["lock_mutex"].is_boolean())
        spdlog::error("[null] lock_mutex in JSON config should be a boolean");
    else
        lock_mutex = config["lock_mutex"].get<bool>();
  }
  auto backend = std::make_unique<NullBackend>(engine, delay_ms, active_delay, lock_mutex);
  spdlog::trace("[null] Successfully opened database");
  return backend;
}

} // namespace sonata
