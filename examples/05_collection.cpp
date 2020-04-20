/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include <iostream>
#include <sonata/Admin.hpp>
#include <sonata/Client.hpp>

namespace tl = thallium;

const char* records[] = {
    R"({ "name" : "John Lennon",
         "year" : 1940
    })",
    R"({ "name" : "Paul McCartney",
         "year" : 1942
    })",
    R"({ "name" : "George Harrison",
         "year" : 1943
    })",
    R"({ "name" : "Peter Best",
         "year" : 1941
    })"
};

void example(sonata::Collection& collection) {

    // Store records
    for(unsigned i=0; i < sizeof(records)/sizeof(records[0]); i++) {
        uint64_t id = collection.store(records[i]);
    }

    // Fetch by id
    std::string paul;
    collection.fetch(1, &paul);

    // Get everybody
    std::vector<std::string> all;
    collection.all(&all);

    // Finds the ones born after 1941
    std::vector<std::string> filtered;
    std::string code = R"(
        function($member) {
            if($member.year > 1941) {
                return TRUE;
            } else {
                return FALSE;
            }
    })";
    collection.filter(code, &filtered);

    // Update a record
    std::string new_drummer =
    R"({ "name" : "Ringo Starr",
         "year" : 1940
    })";
    collection.update(3, new_drummer);

    // Get the last id (should be 3)
    uint64_t last_id = collection.last_record_id();

    // Get the collection size (should be 4)
    size_t size = collection.size();

    // Erase John Lennon
    collection.erase(0);
}

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
        std::string config = "{ \"path\" : \"./mydatabase\" }";
        admin.createDatabase(addr, 0, "bands", "unqlite", config);

        // Initialize a Client
        sonata::Client client(engine);

        // Open the Database "bands" from provider 0
        sonata::Database db = client.open(addr, 0, "bands");

        // Create a collection in the database
        sonata::Collection collection = db.create("beatles");

        // Run a few example queries
        example(collection);

        // Destroy the database
        admin.destroyDatabase(addr, 0, "bands");

        // Any of the above functions may throw a sonata::Exception
    } catch(const sonata::Exception& ex) {
        std::cerr << ex.what() << std::endl;
        exit(-1);
    }

    return 0;
}
