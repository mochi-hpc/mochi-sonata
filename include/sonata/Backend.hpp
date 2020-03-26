#ifndef __SONATA_BACKEND_HPP
#define __SONATA_BACKEND_HPP

#include <sonata/RequestResult.hpp>

#include <json/json.h>

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

    virtual RequestResult<bool> dropCollection(
            const std::string& coll_name) = 0;

    virtual RequestResult<uint64_t> store(
            const std::string& coll_name,
            const std::string& record) = 0;

    virtual RequestResult<std::string> fetch(
            const std::string& coll_name,
            uint64_t record_id) = 0;

    virtual RequestResult<std::string> filter(
            const std::string& coll_name,
            const std::string& filter_code) = 0;

    virtual RequestResult<bool> update(
            const std::string& coll_name,
            uint64_t record_id,
            const std::string& new_content) = 0;

    virtual RequestResult<std::string> all(
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

    public:

    BackendFactory() = delete;

    static std::unique_ptr<Backend> createBackend(const std::string& backend_name,
                                                  const Json::Value& config);

    private:

    static std::unordered_map<std::string,
                std::function<std::unique_ptr<Backend>(const Json::Value&)>> factories;
};

#define SONATA_REGISTER_BACKEND(__backend_name, __backend_type) \
    static __BackendRegistration<__backend_type> __sonata ## __backend_name ## _backend( #__backend_name );

template<typename BackendType>
class __BackendRegistration {
    
    public:

    template<typename FactoryType>
    __BackendRegistration(const std::string& backend_name)
    {
        BackendFactory::factories[backend_name] = [](const Json::Value& config) {
            return BackendType::New(config);
        };
    }
};

}

#endif
