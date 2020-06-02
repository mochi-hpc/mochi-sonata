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
class Database;

/**
 * @brief The Client object is the main object used to establish
 * a connection with a Sonata service.
 */
class Client {

    friend class Database;

    public:

    /**
     * @brief Default constructor.
     */
    Client();

    /**
     * @brief Constructor using a margo instance id.
     *
     * @param mid Margo instance id.
     */
    Client(margo_instance_id mid);

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
     * @brief Returns the thallium engine used by the client.
     */
    const thallium::engine& engine() const;

    /**
     * @brief Returns the thallium engine used by the client.
     */
    thallium::engine& engine();

    /**
     * @brief Opens a remote database and returns a
     * Database instance to access it. You may set "check" to false
     * if you know for sure that the corresponding database exists.
     *
     * @param address Address of the provider holding the database.
     * @param provider_id Provider id.
     * @param db_name Database name.
     * @param check Checks if the Database exists by issuing an RPC.
     *
     * @return a Database instance.
     */
    Database open(const std::string& address, 
                  uint16_t provider_id,
                  const std::string& db_name,
                  bool check = true) const;

    /**
     * @brief Checks that the Client instance is valid.
     */
    operator bool() const;

    private:

    Client(const std::shared_ptr<ClientImpl>& impl);

    std::shared_ptr<ClientImpl> self;
};

}

#endif
