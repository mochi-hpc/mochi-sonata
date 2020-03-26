#ifndef __SONATA_ADMIN_HPP
#define __SONATA_ADMIN_HPP

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

    bool createDatabase(const std::string& address,
                        uint16_t provider_id,
                        const std::string& name,
                        const std::string& path) const;

    private:

    std::shared_ptr<AdminImpl> self;
};

}

#endif
