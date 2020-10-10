/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include <cppunit/extensions/HelperMacros.h>
#include <sonata/Client.hpp>
#include <sonata/Admin.hpp>

extern thallium::engine* engine;
extern std::string db_type;

class ClientTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( ClientTest );
    CPPUNIT_TEST( testOpenDatabase );
    CPPUNIT_TEST_SUITE_END();

    static constexpr const char* db_config = "{ \"path\" : \"mydb\" }";

    public:

    void setUp() {
        sonata::Admin admin(*engine);
        std::string addr = engine->self();
        std::strinf cfg;
        if(db_type == "lazy") {
            cfg += "{ \"backend\" : \"unqlite\", \"config\" : ";
            cfg += db_config;
            cfg += "}";
        } else {
            cfg = db_config;
        }
        admin.createDatabase(addr, 0, "mydb", db_type, cfg);
    }

    void tearDown() {
        sonata::Admin admin(*engine);
        std::string addr = engine->self();
        admin.destroyDatabase(addr, 0, "mydb");
    }

    void testOpenDatabase() {
        sonata::Client client(*engine);
        std::string addr = engine->self();
        
        Database mydb = client.open(addr, 0, "mydb");
        CPPUNIT_ASSERT_MESSAGE(
                "Database mydb should be valid",
                static_cast<bool>(mydb));

        CPPUNIT_ASSERT_THROW_MESSAGE(
                "client.open should fail on non-existing database",
                client.open(addr, 0, "blabla");
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION( ClientTest );
