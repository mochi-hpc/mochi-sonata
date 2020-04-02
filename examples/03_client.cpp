#include <iostream>
#include <sonata/Admin.hpp>
#include <sonata/Client.hpp>

namespace tl = thallium;

int main(int argc, char** argv) {
    
    if(argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <address>" << std::endl;
        exit(-1);
    }
    const char* addr = argv[1];

    // Initialize the thallium server
    tl::engine engine("na+sm", THALLIUM_CLIENT_MODE);

    try {

        // Initialize an Admin
        sonata::Admin admin(engine);

        // Create a database in provider 0
        std::string config = "{ \"path\" : \"./mydatabase\" \"}";
        admin.createDatabase(addr, 0, "mydatabase", "unqlite", config);

        // Initialize a Client
        sonata::Client client(engine);

        // Open the Database "mydatabase" from provider 0
        sonata::Database db = client.open(addr, 0, "mydatabase");

        // Destroy the database
        admin.destroyDatabase(addr, 0, "mydatabase");

        // Any of the above functions may throw a sonata::Exception
    } catch(const sonata::Exception& ex) {
        std::cerr << ex.what() << std::endl;
        exit(-1);
    }

    return 0;
}
