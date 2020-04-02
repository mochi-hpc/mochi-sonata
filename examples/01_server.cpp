#include <iostream>
#include <sonata/Provider.hpp>

namespace tl = thallium;

int main(int argc, char** argv) {
    
    // Initialize the thallium server
    tl::engine engine("na+sm", THALLIUM_SERVER_MODE);
    engine.enable_remote_shutdown();

    // Print the address
    std::cout << "Server running at " << (std::string)engine.self() << std::endl;

    // Initialize a Sonata provider
    sonata::Provider provider(engine);

    // Wait for finalize in the engine's destructor
    return 0;
}
