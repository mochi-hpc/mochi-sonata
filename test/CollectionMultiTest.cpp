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

class CollectionMultiTest : public CppUnit::TestFixture,
                       public CollectionTestBase
{
    CPPUNIT_TEST_SUITE( CollectionMultiTest );
    CPPUNIT_TEST( testStore );
    CPPUNIT_TEST( testFetch );
    CPPUNIT_TEST( testUpdate );
    CPPUNIT_TEST( testErase );
    CPPUNIT_TEST_SUITE_END();

    static constexpr const char* db_config = "{ \"path\" : \"mydb\" }";

    public:

    void setUp() {
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

        // Strings can be stored
        std::vector<uint64_t> record_ids(records_str.size());
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "coll.store should not throw.",
                coll.store_multi(records_str, record_ids.data()));
        for(uint64_t i = 0; i < record_ids.size(); i++) {
            if(db_type == "aggregator") {
                CPPUNIT_ASSERT_EQUAL_MESSAGE(
                    "record id should be correct.",
                    std::numeric_limits<uint64_t>::max(), record_ids[i]);
            } else {
                CPPUNIT_ASSERT_EQUAL_MESSAGE(
                    "record id should be correct.",
                    i, record_ids[i]);
            }
        }

        tearDown();
        setUp();

        // JSON objects can be stored
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "coll.store should not throw.",
                coll.store_multi(records_json_all, record_ids.data()));
        for(uint64_t i = 0; i < record_ids.size(); i++) {
            if(db_type == "aggregator") {
                CPPUNIT_ASSERT_EQUAL_MESSAGE(
                    "record id should be correct.",
                    std::numeric_limits<uint64_t>::max(), record_ids[i]);
            } else {
                CPPUNIT_ASSERT_EQUAL_MESSAGE(
                    "record id should be correct.",
                    i, record_ids[i]);
            }
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

        // Fetch records 0 and 2, which should exist
        json result_json;
        std::vector<uint64_t> ids_to_fetch = { 0, 2};
        std::vector<std::string> result;

        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "coll.fetch_multi should not throw.",
                coll.fetch_multi(ids_to_fetch.data(), 2, &result));
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "result should have 2 elements.",
                (size_t)2, result.size());

        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "coll.fetch_multi should not throw.",
                coll.fetch_multi(ids_to_fetch.data(), 2, &result_json));

        // Fetched record should be as expected
        int i = 0;
        for(uint64_t id : ids_to_fetch) {
            CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "Fetched record should contain correct data.",
                records_json[id]["name"].get<std::string>(),
                result_json[i]["name"].get<std::string>());
            i += 1;
        }

        // Fetch a set of records including one that does not exist
        uint64_t bad_id = records_str.size()+1;
        std::vector<uint64_t> bad_ids = { 2, bad_id };
        CPPUNIT_ASSERT_THROW_MESSAGE(
                "coll.fetch_multi should throw.",
                coll.fetch(bad_id, &result_json),
                sonata::Exception);
    }

    void testUpdate() {
        sonata::Client client(*engine);
        std::string addr = engine->self();
        sonata::Database mydb = client.open(addr, 0, "mydb");
        sonata::Collection coll = mydb.open("mycollection");

        { // string-based functions

        for(const auto& r : records_str) {
            CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                    "coll.store should not throw.",
                    coll.store(r));
        }
//        mydb.commit();
        // Update record 0 and 2 with new content
        std::vector<std::string> new_contents = {
            "{ \"name\" : \"Georges\", \"city\" : \"Lyon\", \"papers\" : 89 }",
            "{ \"name\" : \"Denis\", \"city\" : \"Urbana\", \"papers\" : 65 }"
        };

        std::vector<uint64_t> ids_to_update = { 0, 2 };
        ids_to_update[1] = records_str.size() + 1;

        // Update records include one that does not exist
        std::vector<bool> updated;
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "update should not throw for a record that does not exist.",
                coll.update_multi(ids_to_update.data(), new_contents, &updated));
        CPPUNIT_ASSERT_MESSAGE(
                "vector of bool should be of size 2.",
                updated.size() == 2);

        CPPUNIT_ASSERT_MESSAGE(
                "record 0 should have been updated.",
                updated[0]);
        CPPUNIT_ASSERT_MESSAGE(
                "record 1 should not have been updated.",
                !updated[1]);

  //      mydb.commit();
        // Check the content of record 0, it should have changed
        json val;
        coll.fetch(0, &val);
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "record 0 should not have changed.",
                val["name"].get<std::string>(), std::string("Georges"));

//        mydb.commit();
        // Do a correct update of 2 records
        ids_to_update[1]  = 2;
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "update should not throw.",
                coll.update_multi(ids_to_update.data(), new_contents, &updated));
        CPPUNIT_ASSERT_MESSAGE(
                "vector of bool should be of size 2.",
                updated.size() == 2);

        CPPUNIT_ASSERT_MESSAGE(
                "record 0 should have been updated.",
                updated[0]);
        CPPUNIT_ASSERT_MESSAGE(
                "record 1 should have been updated.",
                updated[1]);

  //      mydb.commit();
        // Check the new content
        coll.fetch(0, &val);
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "fetch should give the updated value.",
                val["name"].get<std::string>(), std::string("Georges"));

        coll.fetch(2, &val);
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "fetch should give the updated value.",
                val["name"].get<std::string>(), std::string("Denis"));

        }

        tearDown();
        setUp();

        { // json-based functions

        for(const auto& r : records_str) {
            CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                    "coll.store should not throw.",
                    coll.store(r));
        }
    //    mydb.commit();

        // Update record 0 and 2 with new content
        json new_contents;
        new_contents[0]["name"] = "Georges";
        new_contents[0]["city"] = "Lyon";
        new_contents[0]["papers"] = 89;
        new_contents[1]["name"] = "Denis";
        new_contents[1]["city"] = "Urbana";
        new_contents[1]["papers"] = 65;

        std::vector<uint64_t> ids_to_update = { 0, 2 };
        ids_to_update[1] = records_str.size() + 1;

        std::vector<bool> updated;

        // Update records include one that does not exist
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "update should throw for a record that does not exist.",
                coll.update_multi(ids_to_update.data(), new_contents, &updated));
        CPPUNIT_ASSERT_MESSAGE(
                "vector of bool should be of size 2.",
                updated.size() == 2);

        CPPUNIT_ASSERT_MESSAGE(
                "record 0 should have been updated.",
                updated[0]);
        CPPUNIT_ASSERT_MESSAGE(
                "record 1 should not have been updated.",
                !updated[1]);

        // Check the content of record 0, it should have changed
        json val;
        coll.fetch(0, &val);
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "record 0 should not have changed.",
                val["name"].get<std::string>(), std::string("Georges"));

        // Do a correct update of 2 records
        ids_to_update[1]  = 2;
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "update should not throw.",
                coll.update_multi(ids_to_update.data(), new_contents, &updated));
        CPPUNIT_ASSERT_MESSAGE(
                "vector of bool should be of size 2.",
                updated.size() == 2);

        CPPUNIT_ASSERT_MESSAGE(
                "record 0 should have been updated.",
                updated[0]);
        CPPUNIT_ASSERT_MESSAGE(
                "record 1 should have been updated.",
                updated[1]);

      //  mydb.commit();
        // Check the new content
        coll.fetch(0, &val);
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "fetch should give the updated value.",
                val["name"].get<std::string>(), std::string("Georges"));

        coll.fetch(2, &val);
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "fetch should give the updated value.",
                val["name"].get<std::string>(), std::string("Denis"));

        } 
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

        // Erase records 0 and 2 record
        std::vector<uint64_t> records_to_erase = {0, 2};
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "erasing should work.",
                coll.erase_multi(records_to_erase.data(), 2));

        // Check that we can't access it anymore
        std::string tmp;
        CPPUNIT_ASSERT_THROW_MESSAGE(
                "record 2 should be inaccessible.",
                coll.fetch(2, &tmp),
                sonata::Exception);

        // Check that the size is appropriate
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "size of collection should be correct.",
                (int)records_str.size()-2, (int)coll.size());
    }

};
CPPUNIT_TEST_SUITE_REGISTRATION( CollectionMultiTest );
