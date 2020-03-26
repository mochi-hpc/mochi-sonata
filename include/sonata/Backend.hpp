#ifndef __SONATA_BACKEND_HPP
#define __SONATA_BACKEND_HPP

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

    virtual bool createCollection(
            const std::string& coll_name) = 0;

    virtual bool dropCollection(
            const std::string& coll_name) = 0;

    virtual std::pair<bool,uint64_t> store(
            const std::string& coll_name,
            const std::string& record) = 0;

    virtual std::pair<bool,std::string> fetch(
            const std::string& coll_name,
            uint64_t record_id) = 0;

    virtual std::pair<bool,std::string> filter(
            const std::string& coll_name,
            const std::string& filter_code) = 0;

    virtual bool update(
            const std::string& coll_name,
            uint64_t record_id,
            const std::string& new_content) = 0;

    virtual std::pair<bool,std::string> all(
            const std::string& coll_name) = 0;

    virtual uint64_t lastID(
            const std::string& coll_name) = 0;

    virtual size_t size(
            const std::string& coll_name) = 0;

    virtual bool erase(
            const std::string& coll_name,
            uint64_t record_id) = 0;

    virtual bool destroy() = 0;

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
