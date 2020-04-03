/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_CLIENT_HPP
#define __SONATA_CLIENT_HPP

#include <sonata/Database.hpp>
#include <thallium.hpp>
#include <memory>

namespace sonata {

class ClientImpl;

/**
 * @brief The Client object is the main object used to establish
 * a connection with a Sonata service.
 */
class Client {

    public:

    /**
     * @brief Constructor.
     *
     * @param engine Thallium engine.
     */
    Client(thallium::engine& engine);
    
    /**
     * @brief Copy constructor.
     */
    Client(const Client&);

    /**
     * @brief Move constructor.
     */
    Client(Client&&);

    /**
     * @brief Copy-assignment operator.
     */
    Client& operator=(const Client&);

    /**
     * @brief Move-assignment operator.
     */
    Client& operator=(Client&&);

    /**
     * @brief Destructor.
     */
    ~Client();

    /**
     * @brief Opens a remote database and returns a
     * Database instance to access it.
     *
     * @param address Address of the provider holding the database.
     * @param provider_id Provider id.
     * @param db_name Database name.
     *
     * @return a Database instance.
     */
    Database open(const std::string& address, 
                  uint16_t provider_id,
                  const std::string& db_name) const;

    /**
     * @brief Checks that the Client instance is valid.
     */
    operator bool() const;

    private:

    std::shared_ptr<ClientImpl> self;
};

}

#endif
