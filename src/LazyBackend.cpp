/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "LazyBackend.hpp"

namespace sonata {

namespace tl = thallium;
using namespace std::string_literals;

SONATA_REGISTER_BACKEND(lazy, LazyBackend);

std::unique_ptr<Backend> LazyBackend::create(const tl::engine& engine, const tl::pool& pool, const Json::Value& config) {
    spdlog::trace("[lazy] Creating Cached database");
    std::string backend_type = config.get("backend", "").asString();
    if(backend_type.size() == 0) {
        throw Exception("LazyBackend needs to be initialized with a \"backend\" entry");
    }
    bool flush_on_read = config.get("flush-on-read", true).asBool();
    bool flush_on_exec = config.get("flush-on-exec", true).asBool();
    const auto& inner_cfg = config["config"];
    std::unique_ptr<Backend> inner = BackendFactory::createBackend(backend_type, engine, pool, inner_cfg);
    auto backend = std::make_unique<LazyBackend>(std::move(inner), pool, flush_on_read, flush_on_exec);
    spdlog::trace("[lazy] Successfully created database");
    return backend;
}

std::unique_ptr<Backend> LazyBackend::attach(const thallium::engine& engine, const tl::pool& pool, const Json::Value& config) {
    spdlog::trace("[lazy] Opening Cached database");
    std::string backend_type = config.get("backend", "").asString();
    if(backend_type.size() == 0) {
        throw Exception("LazyBackend needs to be initialized with a \"backend\" entry");
    }
    const auto& inner_cfg = config["config"];
    bool flush_on_read = config.get("flush-on-read", true).asBool();
    bool flush_on_exec = config.get("flush-on-exec", true).asBool();
    std::unique_ptr<Backend> inner = BackendFactory::attachBackend(backend_type, engine, pool, inner_cfg);
    auto backend = std::make_unique<LazyBackend>(std::move(inner), pool, flush_on_read, flush_on_exec);
    spdlog::trace("[lazy] Successfully opened database");
    return backend;
}

}
