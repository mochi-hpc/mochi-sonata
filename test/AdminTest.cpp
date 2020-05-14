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
    CPPUNIT_TEST( testAdminCreateTwoDatabases );
    CPPUNIT_TEST_SUITE_END();

    static constexpr const char* db_config = "{ \"path\" : \"mydb\" }";

    public:

    void setUp() {}
    void tearDown() {}

    void testAdminCreateDatabase() {
        sonata::Admin admin(*engine);
        std::string addr = engine->self();

        // Create a valid Database
        CPPUNIT_ASSERT_NO_THROW_MESSAGE("admin.createDatabase should return a valid Database",
                admin.createDatabase(addr, 0, "db1", db_type, db_config));

        // Create a Database with a name already taken
        CPPUNIT_ASSERT_THROW_MESSAGE("admin.createDatabase should throw an exception (wrong name)",
                admin.createDatabase(addr, 0, "db1", db_type, db_config),
                sonata::Exception);

        // Create a Database with a wrong backend type
        CPPUNIT_ASSERT_THROW_MESSAGE("admin.createDatabase should throw an exception (wrong backend)",
                admin.createDatabase(addr, 0, "db_blabla", "blabla", db_config),
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

    void testAdminCreateTwoDatabases() {
        sonata::Admin admin(*engine);
        std::string addr = engine->self();

        // Create two valid Database
        CPPUNIT_ASSERT_NO_THROW_MESSAGE("admin.createDatabase should return a valid Database",
                admin.createDatabase(addr, 0, "db1", db_type, "{ \"path\" : \"mydb1\" }"));
        CPPUNIT_ASSERT_NO_THROW_MESSAGE("admin.createDatabase should return a valid Database",
                admin.createDatabase(addr, 0, "db2", db_type, "{ \"path\" : \"mydb2\" }"));
        CPPUNIT_ASSERT_NO_THROW_MESSAGE("admin.createDatabase should return a valid Database",
                admin.createDatabase(addr, 0, "db3", db_type, "{ \"path\" : \"mydb3\" }"));
        CPPUNIT_ASSERT_NO_THROW_MESSAGE("admin.createDatabase should return a valid Database",
                admin.createDatabase(addr, 0, "db4", db_type, "{ \"path\" : \"mydb4\" }"));
        CPPUNIT_ASSERT_NO_THROW_MESSAGE("admin.createDatabase should return a valid Database",
                admin.createDatabase(addr, 0, "db5", db_type, "{ \"path\" : \"mydb5\" }"));
        CPPUNIT_ASSERT_NO_THROW_MESSAGE("admin.createDatabase should return a valid Database",
                admin.createDatabase(addr, 0, "db6", db_type, "{ \"path\" : \"mydb6\" }"));
        CPPUNIT_ASSERT_NO_THROW_MESSAGE("admin.createDatabase should return a valid Database",
                admin.createDatabase(addr, 0, "db7", db_type, "{ \"path\" : \"mydb7\" }"));
        CPPUNIT_ASSERT_NO_THROW_MESSAGE("admin.createDatabase should return a valid Database",
                admin.createDatabase(addr, 0, "db8", db_type, "{ \"path\" : \"mydb8\" }"));
        CPPUNIT_ASSERT_NO_THROW_MESSAGE("admin.createDatabase should return a valid Database",
                admin.createDatabase(addr, 0, "db9", db_type, "{ \"path\" : \"mydb9\" }"));
        CPPUNIT_ASSERT_NO_THROW_MESSAGE("admin.createDatabase should return a valid Database",
                admin.createDatabase(addr, 0, "db10", db_type, "{ \"path\" : \"mydb10\" }"));
        CPPUNIT_ASSERT_NO_THROW_MESSAGE("admin.createDatabase should return a valid Database",
                admin.createDatabase(addr, 0, "db11", db_type, "{ \"path\" : \"mydb11\" }"));
        CPPUNIT_ASSERT_NO_THROW_MESSAGE("admin.createDatabase should return a valid Database",
                admin.createDatabase(addr, 0, "db12", db_type, "{ \"path\" : \"mydb12\" }"));
        CPPUNIT_ASSERT_NO_THROW_MESSAGE("admin.createDatabase should return a valid Database",
                admin.createDatabase(addr, 0, "db13", db_type, "{ \"path\" : \"mydb13\" }"));
        CPPUNIT_ASSERT_NO_THROW_MESSAGE("admin.createDatabase should return a valid Database",
                admin.createDatabase(addr, 0, "db14", db_type, "{ \"path\" : \"mydb14\" }"));
        CPPUNIT_ASSERT_NO_THROW_MESSAGE("admin.createDatabase should return a valid Database",
                admin.createDatabase(addr, 0, "db15", db_type, "{ \"path\" : \"mydb15\" }"));
        CPPUNIT_ASSERT_NO_THROW_MESSAGE("admin.createDatabase should return a valid Database",
                admin.createDatabase(addr, 0, "db16", db_type, "{ \"path\" : \"mydb16\" }"));
        CPPUNIT_ASSERT_NO_THROW_MESSAGE("admin.createDatabase should return a valid Database",
                admin.createDatabase(addr, 0, "db17", db_type, "{ \"path\" : \"mydb17\" }"));

        // Destroy the Databases
        admin.destroyDatabase(addr, 0, "db1");
        admin.destroyDatabase(addr, 0, "db2");
        admin.destroyDatabase(addr, 0, "db3");
        admin.destroyDatabase(addr, 0, "db4");
        admin.destroyDatabase(addr, 0, "db5");
        admin.destroyDatabase(addr, 0, "db6");
        admin.destroyDatabase(addr, 0, "db7");
        admin.destroyDatabase(addr, 0, "db8");
        admin.destroyDatabase(addr, 0, "db9");
        admin.destroyDatabase(addr, 0, "db10");
        admin.destroyDatabase(addr, 0, "db11");
        admin.destroyDatabase(addr, 0, "db12");
        admin.destroyDatabase(addr, 0, "db13");
        admin.destroyDatabase(addr, 0, "db14");
        admin.destroyDatabase(addr, 0, "db15");
        admin.destroyDatabase(addr, 0, "db16");
        admin.destroyDatabase(addr, 0, "db17");
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION( AdminTest );
