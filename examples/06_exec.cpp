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

void example(sonata::Database& db) {
    // Create the database
    sonata::Collection collection = db.create("beatles");
    // Store records
    for(unsigned i=0; i < sizeof(records)/sizeof(records[0]); i++) {
        uint64_t id = collection.store(records[i]);
    }
    // Execute a complex query
    std::string code =
    R"(
        db_store('beatles', { name : 'Stuart Sutcliffe', year : 1940 });
        $years = [];
        while(($member = db_fetch('beatles')) != NULL) {
            printf("%s\n", $member.name);
            array_push($years, $member.year);
        }
    )";
    std::unordered_set<std::string> vars;
    vars.insert("years"); // Get the years variable back
    vars.insert("__output__"); // Get the VM's standard output
    std::unordered_map<std::string, std::string> result;
    db.execute(code, vars, &result);
    std::cout << result["years"] << std::endl;
    std::cout << result["__output__"] << std::endl;
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

        // Run a few example queries
        example(db);

        // Destroy the database
        admin.destroyDatabase(addr, 0, "bands");

        // Any of the above functions may throw a sonata::Exception
    } catch(const sonata::Exception& ex) {
        std::cerr << ex.what() << std::endl;
        exit(-1);
    }

    return 0;
}
