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

class DatabaseTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( DatabaseTest );
    CPPUNIT_TEST( testCreateCollection );
    CPPUNIT_TEST( testOpenCollection );
    CPPUNIT_TEST( testCollectionExists );
    CPPUNIT_TEST( testDropCollection );
    CPPUNIT_TEST_SUITE_END();

    static constexpr const char* db_config = "{ \"path\" : \"mydb\" }";

    public:

    void setUp() {
        sonata::Admin admin(*engine);
        std::string addr = engine->self();
        std::string cfg;
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

    void testCreateCollection() {
        sonata::Client client(*engine);
        std::string addr = engine->self();
        
        sonata::Database mydb = client.open(addr, 0, "mydb");

        // Creating a collection
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "mydb.create should not throw.",
                mydb.create("mycollection"));

        // Creating a collection with the same name
        CPPUNIT_ASSERT_THROW_MESSAGE(
                "mydb.create should fail because the collection already exists.",
                mydb.create("mycollection"),
                sonata::Exception);
    }

    void testCollectionExists() {
        sonata::Client client(*engine);
        std::string addr = engine->self();
        
        sonata::Database mydb = client.open(addr, 0, "mydb");

        // Creating a collection
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "mydb.create should not throw.",
                mydb.create("mycollection"));

        // Check if a collection exists
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "mycollection should exist.",
                true, mydb.exists("mycollection"));

        // Check that a collection does not exist
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "blabla collection should not exist.",
                false, mydb.exists("blabla"));
    }

    void testOpenCollection() {
        sonata::Client client(*engine);
        std::string addr = engine->self();
        
        sonata::Database mydb = client.open(addr, 0, "mydb");

        // Creating a collection
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "mydb.create should not throw.",
                mydb.create("mycollection"));

        // Open a collection that exist
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "mydb.open should not throw.",
                mydb.open("mycollection"));

        // Open a collection that does not exist
        CPPUNIT_ASSERT_THROW_MESSAGE(
                "mydb.open should throw because collectiom blabla does not exist.",
                mydb.open("blabla"),
                sonata::Exception);
    }

    void testDropCollection() {
        sonata::Client client(*engine);
        std::string addr = engine->self();
        
        sonata::Database mydb = client.open(addr, 0, "mydb");

        // Creating a collection
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "mydb.create should not throw.",
                mydb.create("mycollection"));

        // Check if a collection exists
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "mycollection should exist.",
                true, mydb.exists("mycollection"));

        // Dropping the collection
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "dropping mycollection should not throw.",
                mydb.drop("mycollection"));

        // Check that the collection does not exist anymore
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "mycollection collection should not exist.",
                false, mydb.exists("mycollection"));

        // Check that we throw if we drop it twice
        CPPUNIT_ASSERT_THROW_MESSAGE(
                "dropping collection twice should throw.",
                 mydb.drop("mycollection"),
                 sonata::Exception);
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION( DatabaseTest );
