#include "sonata/Backend.hpp"
#include <json/json.h>

namespace sonata {

class UnQLiteBackend : public Backend {
    
    public:

    UnQLiteBackend(const Json::Value& config);

    UnQLiteBackend(UnQLiteBackend&&) = delete;

    UnQLiteBackend(const UnQLiteBackend&) = delete;

    UnQLiteBackend& operator=(UnQLiteBackend&&) = delete;

    UnQLiteBackend& operator=(const UnQLiteBackend&) = delete;

    virtual ~UnQLiteBackend() = default;

    virtual RequestResult<bool> createCollection(
            const std::string& coll_name) override;

    virtual RequestResult<bool> dropCollection(
            const std::string& coll_name) override;

    virtual RequestResult<uint64_t> store(
            const std::string& coll_name,
            const std::string& record) override;

    virtual RequestResult<std::string> fetch(
            const std::string& coll_name,
            uint64_t record_id) override;

    virtual RequestResult<std::string> filter(
            const std::string& coll_name,
            const std::string& filter_code) override;

    virtual RequestResult<bool> update(
            const std::string& coll_name,
            uint64_t record_id,
            const std::string& new_content) override;

    virtual RequestResult<std::string> all(
            const std::string& coll_name) override;

    virtual RequestResult<uint64_t> lastID(
            const std::string& coll_name) override;

    virtual RequestResult<size_t> size(
            const std::string& coll_name) override;

    virtual RequestResult<bool> erase(
            const std::string& coll_name,
            uint64_t record_id) override;

    virtual RequestResult<bool> destroy() override;

};

SONATA_REGISTER_BACKEND(unqlite, UnQLiteBackend);

}

