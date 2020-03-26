#ifndef __SONATA_ADMIN_HPP
#define __SONATA_ADMIN_HPP

#include <json/json.h>
#include <thallium.hpp>
#include <memory>

namespace sonata {

class AdminImpl;

class Admin {

    public:

    Admin(thallium::engine& engine, const std::string& token);
    
    Admin(const Admin&);

    Admin(Admin&&);

    Admin& operator=(const Admin&);

    Admin& operator=(Admin&&);

    ~Admin();

    operator bool() const;

    void attachDatabase(const std::string& address,
                        uint16_t provider_id,
                        const std::string& name,
                        const std::string& type,
                        const std::string& config) const;

    void attachDatabase(const std::string& address,
                        uint16_t provider_id,
                        const std::string& name,
                        const std::string& type,
                        const Json::Value& config) const;

    void detachDatabase(const std::string& address,
                        uint16_t provider_id,
                        const std::string& name) const;

    void destroyDatabase(const std::string& address,
                         uint16_t provider_id,
                         const std::string& name) const;

    private:

    std::shared_ptr<AdminImpl> self;
};

}

#endif
