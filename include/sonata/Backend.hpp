#ifndef __SONATA_BACKEND_HPP
#define __SONATA_BACKEND_HPP

#include <sonata/RequestResult.hpp>
#include <unordered_map>
#include <functional>
#include <json/json.h>

template<typename BackendType>
class __SonataBackendRegistration;

namespace sonata {

class Backend {
    
    public:

    Backend() = default;

    Backend(Backend&&) = default;

    Backend(const Backend&) = default;

    Backend& operator=(Backend&&) = default;

    Backend& operator=(const Backend&) = default;

    virtual ~Backend() = default;

    virtual RequestResult<bool> createCollection(
            const std::string& coll_name) = 0;

    virtual RequestResult<bool> openCollection(
            const std::string& coll_name) = 0;

    virtual RequestResult<bool> dropCollection(
            const std::string& coll_name) = 0;

    virtual RequestResult<uint64_t> store(
            const std::string& coll_name,
            const std::string& record) = 0;

    virtual RequestResult<std::string> fetch(
            const std::string& coll_name,
            uint64_t record_id) = 0;

    virtual RequestResult<std::vector<std::string>> filter(
            const std::string& coll_name,
            const std::string& filter_code) = 0;

    virtual RequestResult<bool> update(
            const std::string& coll_name,
            uint64_t record_id,
            const std::string& new_content) = 0;

    virtual RequestResult<std::vector<std::string>> all(
            const std::string& coll_name) = 0;

    virtual RequestResult<uint64_t> lastID(
            const std::string& coll_name) = 0;

    virtual RequestResult<size_t> size(
            const std::string& coll_name) = 0;

    virtual RequestResult<bool> erase(
            const std::string& coll_name,
            uint64_t record_id) = 0;

    virtual RequestResult<bool> destroy() = 0;

};

class BackendFactory {

    template<typename BackendType>
    friend class ::__SonataBackendRegistration;

    public:

    BackendFactory() = delete;

    static std::unique_ptr<Backend> createBackend(const std::string& backend_name,
                                                  const Json::Value& config);

    static std::unique_ptr<Backend> attachBackend(const std::string& backend_name,
                                                  const Json::Value& config);

    private:

    static std::unordered_map<std::string,
                std::function<std::unique_ptr<Backend>(const Json::Value&)>> create_fn;
    
    static std::unordered_map<std::string,
                std::function<std::unique_ptr<Backend>(const Json::Value&)>> attach_fn;
};

} // namespace sonata


#define SONATA_REGISTER_BACKEND(__backend_name, __backend_type) \
    static __SonataBackendRegistration<__backend_type> __sonata ## __backend_name ## _backend( #__backend_name )

template<typename BackendType>
class __SonataBackendRegistration {
    
    public:

    __SonataBackendRegistration(const std::string& backend_name)
    {
        sonata::BackendFactory::create_fn[backend_name] = [](const Json::Value& config) {
            return BackendType::create(config);
        };
        sonata::BackendFactory::attach_fn[backend_name] = [](const Json::Value& config) {
            return BackendType::attach(config);
        };
    }
};

#endif
