#include "sonata/Backend.hpp"

namespace sonata {

std::unordered_map<std::string,
                std::function<std::unique_ptr<Backend>(const Json::Value&)>> BackendFactory::create_fn;

std::unordered_map<std::string,
                std::function<std::unique_ptr<Backend>(const Json::Value&)>> BackendFactory::attach_fn;

std::unique_ptr<Backend> BackendFactory::createBackend(const std::string& backend_name,
                                                       const Json::Value& config) {
    auto it = create_fn.find(backend_name);
    if(it == create_fn.end()) return nullptr;
    auto& f = it->second;
    return f(config);
}

std::unique_ptr<Backend> BackendFactory::attachBackend(const std::string& backend_name,
                                                       const Json::Value& config) {
    auto it = attach_fn.find(backend_name);
    if(it == attach_fn.end()) return nullptr;
    auto& f = it->second;
    return f(config);
}

}
