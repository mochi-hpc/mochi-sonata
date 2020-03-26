#ifndef __SONATA_CLIENT_HPP
#define __SONATA_CLIENT_HPP

#include <sonata/Database.hpp>
#include <thallium.hpp>
#include <memory>

namespace sonata {

class ClientImpl;

class Client {

    public:

    Client(thallium::engine& engine);
    
    Client(const Client&);

    Client(Client&&);

    Client& operator=(const Client&);

    Client& operator=(Client&&);

    ~Client();

    Database open(const std::string& address, 
                  uint16_t provider_id,
                  const std::string& db_name) const;

    operator bool() const;

    private:

    std::shared_ptr<ClientImpl> self;
};

}

#endif
