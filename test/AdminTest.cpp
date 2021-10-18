/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include <sonata/Admin.hpp>
#include <cppunit/extensions/HelperMacros.h>

extern thallium::engine* engine;
extern std::string db_type;

class AdminTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( AdminTest );
    CPPUNIT_TEST( testAdminCreateDatabase );
    CPPUNIT_TEST_SUITE_END();

    static constexpr const char* db_config = "{ \"path\" : \"mydb\" }";

    public:

    void setUp() {}
    void tearDown() {}

    void testAdminCreateDatabase() {
        sonata::Admin admin(*engine);
        std::string addr = engine->self();

        std::string cfg;
        if(db_type == "aggregator") {
            cfg += "{ \"backend\" : \"unqlite\", \"config\" : ";
            cfg += db_config;
            cfg += "}";
        } else {
            cfg = db_config;
        }

        // Create a valid Database
        CPPUNIT_ASSERT_NO_THROW_MESSAGE("admin.createDatabase should return a valid Database",
                admin.createDatabase(addr, 0, "db1", db_type, cfg));

        // Create a Database with a name already taken
        CPPUNIT_ASSERT_THROW_MESSAGE("admin.createDatabase should throw an exception (wrong name)",
                admin.createDatabase(addr, 0, "db1", db_type, cfg),
                sonata::Exception);

        // Create a Database with a wrong backend type
        CPPUNIT_ASSERT_THROW_MESSAGE("admin.createDatabase should throw an exception (wrong backend)",
                admin.createDatabase(addr, 0, "db_blabla", "blabla", cfg),
                sonata::Exception);

        // Create a Database with a wrong configuration
        if(db_type == "unqlite") {
            CPPUNIT_ASSERT_THROW_MESSAGE("admin.createDatabase should throw an exception (wrong config)",
                admin.createDatabase(addr, 0, "db_no_config", db_type, ""),
                sonata::Exception);
        }

        // Destroy the Database
        admin.destroyDatabase(addr, 0, "db1");
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION( AdminTest );
