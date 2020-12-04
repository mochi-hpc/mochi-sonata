/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <fstream>
#include <cppunit/CompilerOutputter.h>
#include <cppunit/XmlOutputter.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>
#include <spdlog/spdlog.h>

#include <sonata/Client.hpp>
#include <sonata/Admin.hpp>
#include <sonata/Provider.hpp>

namespace tl = thallium;

tl::engine* engine = nullptr;

class ProviderTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( ProviderTest );
    CPPUNIT_TEST( testCreateProviderWithConfig );
    CPPUNIT_TEST( testFailingCreateProviderWithConfig );
    CPPUNIT_TEST_SUITE_END();

    public:

    void setUp() {}
    void tearDown() {}

    void testCreateProviderWithConfig() {
        const std::string config = R"(
        { "databases" : [
            { "name" : "test-db1",
              "type" : "null",
              "mode" : "create",
              "config" : {}
            },
            { "name" : "test-db2",
              "type" : "jsoncpp",
              "mode" : "create",
              "config" : {}
            }
          ]
        }
        )";

        sonata::Provider provider(*engine, 0, config);
    }

    void testFailingCreateProviderWithConfig() {
        CPPUNIT_ASSERT_THROW(sonata::Provider(*engine,1, "{"), sonata::Exception);
        CPPUNIT_ASSERT_THROW(sonata::Provider(*engine,2, "{ \"databases\" : {}}"), sonata::Exception);
        CPPUNIT_ASSERT_THROW(sonata::Provider(*engine,3,
            "{ \"databases\" : [{ \"name\" : \"\"}]}"), sonata::Exception);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( ProviderTest );

int main(int argc, char** argv) {

    // Initialize logging
    spdlog::set_level(spdlog::level::trace);

    // Get the top level suite from the registry
    CppUnit::Test *suite = CppUnit::TestFactoryRegistry::getRegistry().makeTest();

    // Adds the test to the list of test to run
    CppUnit::TextUi::TestRunner runner;
    runner.addTest( suite );

    std::ofstream xmlOutFile;
    if(argc >= 2) {
        const char* xmlOutFileName = argv[1];
        xmlOutFile.open(xmlOutFileName);
        // Change the default outputter to output XML results into a file
        runner.setOutputter(new CppUnit::XmlOutputter(&runner.result(), xmlOutFile));
    } else {
        // Change the default outputter to output XML results into stderr
        runner.setOutputter(new CppUnit::XmlOutputter(&runner.result(), std::cerr));
    }

    // Initialize the thallium server
    tl::engine theEngine("na+sm", THALLIUM_SERVER_MODE);
    engine = &theEngine;

    // Run the tests.
    bool wasSucessful = runner.run();

    // Finalize the engine
    theEngine.finalize();

    if(argc >= 2)
       xmlOutFile.close();

    // Return error code 1 if the one of test failed.
    return wasSucessful ? 0 : 1;
}
