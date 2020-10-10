/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include <cppunit/extensions/HelperMacros.h>
#include <sonata/Client.hpp>
#include <sonata/Admin.hpp>
#include "CollectionTestBase.hpp"

extern thallium::engine* engine;
extern std::string db_type;

class CollectionTest : public CppUnit::TestFixture,
                       public CollectionTestBase
{
    CPPUNIT_TEST_SUITE( CollectionTest );
    CPPUNIT_TEST( testStore );
    CPPUNIT_TEST( testStoreAsync );
    CPPUNIT_TEST( testFetch );
    CPPUNIT_TEST( testFilter );
    CPPUNIT_TEST( testUpdate );
    CPPUNIT_TEST( testAll );
    CPPUNIT_TEST( testLastRecordID );
    CPPUNIT_TEST( testSize );
    CPPUNIT_TEST( testErase );
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

        sonata::Client client(*engine);
        auto db = client.open(addr, 0, "mydb");
        db.create("mycollection");
    }

    void tearDown() {
        sonata::Admin admin(*engine);
        std::string addr = engine->self();
        admin.destroyDatabase(addr, 0, "mydb");
    }

    void testStore() {
        sonata::Client client(*engine);
        std::string addr = engine->self();
        sonata::Database mydb = client.open(addr, 0, "mydb");
        sonata::Collection coll = mydb.open("mycollection");

        uint64_t ref_record_id = 0;
        // Strings can be stored
        for(const auto& r : records_str) {
            uint64_t record_id;
            CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                    "coll.store should not throw.",
                    record_id = coll.store(r));
            CPPUNIT_ASSERT_EQUAL_MESSAGE(
                    "record id should be correct.",
                    ref_record_id, record_id);
            ref_record_id += 1;
        }

        // JSON objects can be stored
        for(const auto& r : records_json) {
            uint64_t record_id;
            CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                    "coll.store should not throw.",
                    record_id = coll.store(r));
            CPPUNIT_ASSERT_EQUAL_MESSAGE(
                    "record id should be correct.",
                    ref_record_id, record_id);
            ref_record_id += 1;
        }
    }

    void testStoreAsync() {
        sonata::Client client(*engine);
        std::string addr = engine->self();
        sonata::Database mydb = client.open(addr, 0, "mydb");
        sonata::Collection coll = mydb.open("mycollection");

        uint64_t ref_record_id = 0;
        std::vector<uint64_t> ids(records_str.size());
        std::vector<sonata::AsyncRequest> requests(records_str.size());
        // Strings can be stored
        size_t i = 0;
        for(const auto& r : records_str) {
            CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                    "coll.store should not throw.",
                    coll.store(r, &ids[i], false, &requests[i]));
            i += 1;
        }
        for(size_t j = 0; j < i; j++) {
            CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                    "request.wait should not throw.",
                    requests[j].wait());
            CPPUNIT_ASSERT_EQUAL_MESSAGE(
                    "record id should be correct.",
                    (uint64_t)j, ids[j]);
        }
    }

    void testFetch() {
        sonata::Client client(*engine);
        std::string addr = engine->self();
        sonata::Database mydb = client.open(addr, 0, "mydb");
        sonata::Collection coll = mydb.open("mycollection");

        for(const auto& r : records_str) {
            coll.store(r);
        }

        // Fetch a record that should exist
        Json::Value result;
        uint64_t id = records_str.size()/2;
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "coll.fetch should not throw.",
                coll.fetch(id, &result));

        // Fetched record should be as expected
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "Fetched record should contain correct data.",
                records_json[id]["name"].asString(),
                result["name"].asString());

        // Fetch a record that does not exist
        uint64_t bad_id = records_str.size();
        CPPUNIT_ASSERT_THROW_MESSAGE(
                "coll.fetch should throw.",
                coll.fetch(bad_id, &result),
                sonata::Exception);
    }

    void testFilter() {
        if(db_type != "unqlite")
            return;

        sonata::Client client(*engine);
        std::string addr = engine->self();
        sonata::Database mydb = client.open(addr, 0, "mydb");
        sonata::Collection coll = mydb.open("mycollection");

        std::string code = "function($rec) { return $rec.papers > 35; }";
        // Try to filter from an empty collection
        std::vector<std::string> results;

        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "it should be possible to filter from an empty collection.",
                coll.filter(code, &results));
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "result should have no records.",
                0, (int)results.size());

        for(const auto& r : records_str) {
            CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                    "coll.store should not throw.",
                    coll.store(r));

        }
        // Filter that returns half of the records
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "it should be possible to filter from an empty collection.",
                coll.filter(code, &results));
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "result should have 2 records.",
                2, (int)results.size());
        
        // Filter that returns no record
        code = "function($rec) { return $rec.papers > 1000; }";
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "it should be possible to return no result.",
                coll.filter(code, &results));
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "result should have 0 records.",
                0, (int)results.size());

        // Filter into a Json result
        code = "function($rec) { return $rec.papers > 35; }";
        Json::Value json_result;
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "it should be possible to filter from an empty collection.",
                coll.filter(code, &json_result));
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "result should have 2 records.",
                2, (int)json_result.size());
    }

    void testUpdate() {
        sonata::Client client(*engine);
        std::string addr = engine->self();
        sonata::Database mydb = client.open(addr, 0, "mydb");
        sonata::Collection coll = mydb.open("mycollection");

        for(const auto& r : records_str) {
            CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                    "coll.store should not throw.",
                    coll.store(r));
        }

        // Update record 0 with new content
        std::string new_content = "{ \"name\" : \"Georges\", \"city\" : \"Lyon\", \"papers\" : 89 }";
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "update should not throw.",
                coll.update(0, new_content));

        // Check the new content
        Json::Value val;
        coll.fetch(0, &val);
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "fetch should give the updated value.",
                val["name"].asString(), std::string("Georges"));

        // Update a record that does not exist
        CPPUNIT_ASSERT_THROW_MESSAGE(
                "update should throw for a record that does not exist.",
                coll.update(records_str.size(), new_content),
                sonata::Exception);

        // Update with a Json record
        Json::Value new_content_json;
        new_content_json["name"] = "Bob";
        new_content_json["city"] = "London";
        new_content_json["papers"] = 4;
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "update should not throw.",
                coll.update(0, new_content_json));

        // Check the new content
        Json::Value val2;
        coll.fetch(0, &val2);
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "fetch should give the updated value.",
                val2["name"].asString(), std::string("Bob"));
    }

    void testAll() {
        sonata::Client client(*engine);
        std::string addr = engine->self();
        sonata::Database mydb = client.open(addr, 0, "mydb");
        sonata::Collection coll = mydb.open("mycollection");

        // Get all items from an empty collection, as JSON objects
        Json::Value result_json;
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "coll.all should not throw on empty collection.",
                coll.all(&result_json));
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "resulting vector should have 0 element.",
                0, (int)result_json.size());
        // Get all items from an empty collection, as strings
        std::vector<std::string> result_str;
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "coll.all should not throw on empty collection.",
                coll.all(&result_str));
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "resulting vector should have 0 element.",
                0, (int)result_str.size());
        // Store records into the collection
        for(const auto& r : records_str) {
            coll.store(r);
        }

        // Get all items as JSON objects
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "coll.all should not throw on empty collection.",
                coll.all(&result_json));
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "resulting vector should have the correct number of elements.",
                (int)records_json.size(), (int)result_json.size());

        // Check content of the items
        for(unsigned i=0; i < result_json.size(); i++) {
            CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "items should have the same name as stored.",
                records_json[i]["name"].asString(),
                result_json[i]["name"].asString());
        }

        // Get all items as strings
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "coll.all should not throw on empty collection.",
                coll.all(&result_str));
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "resulting vector should have the correct number of element.",
                records_str.size(), result_str.size());
    }

    void testLastRecordID() {
        sonata::Client client(*engine);
        std::string addr = engine->self();
        sonata::Database mydb = client.open(addr, 0, "mydb");
        sonata::Collection coll = mydb.open("mycollection");

        for(const auto& r : records_str) {
            CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                    "coll.store should not throw.",
                    coll.store(r));
        }
        // Check that the last record id of the collection is correct
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "coll.size should be correct.",
                (int)records_str.size()-1, (int)coll.last_record_id());
    }

    void testSize() {
        sonata::Client client(*engine);
        std::string addr = engine->self();
        sonata::Database mydb = client.open(addr, 0, "mydb");
        sonata::Collection coll = mydb.open("mycollection");

        for(const auto& r : records_str) {
            CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                    "coll.store should not throw.",
                    coll.store(r));
        }

        // Check that the size of the collection is correct
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "coll.size should be correct.",
                (int)records_str.size(), (int)coll.size());
    }

    void testErase() {
        sonata::Client client(*engine);
        std::string addr = engine->self();
        sonata::Database mydb = client.open(addr, 0, "mydb");
        sonata::Collection coll = mydb.open("mycollection");

        for(const auto& r : records_str) {
            CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                    "coll.store should not throw.",
                    coll.store(r));
        }

        // Erase the first record
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "erasing should work.",
                coll.erase(0));

        // Check that we can't access it anymore
        std::string tmp;
        CPPUNIT_ASSERT_THROW_MESSAGE(
                "record 0 should be inaccessible.",
                coll.fetch(0, &tmp),
                sonata::Exception);

        // Check that the size is appropriate
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "size of collection should be correct.",
                (int)records_str.size()-1, (int)coll.size());
    }

};
CPPUNIT_TEST_SUITE_REGISTRATION( CollectionTest );
