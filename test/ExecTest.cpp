/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include <cppunit/extensions/HelperMacros.h>
#include <sonata/Client.hpp>
#include <sonata/Admin.hpp>
#include <spdlog/spdlog.h>

extern thallium::engine* engine;

class ExecTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( ExecTest );
    CPPUNIT_TEST( testExec );
    CPPUNIT_TEST_SUITE_END();

    static constexpr const char* db_config = "{ \"path\" : \"mydb\" }";

    public:

    void setUp() {
        sonata::Admin admin(*engine);
        std::string addr = engine->self();
        admin.createDatabase(addr, 0, "mydb", "unqlite", db_config);
    }

    void tearDown() {
        sonata::Admin admin(*engine);
        std::string addr = engine->self();
        admin.destroyDatabase(addr, 0, "mydb");
    }

    void testExec() {
        sonata::Client client(*engine);
        std::string addr = engine->self();
        
        sonata::Database mydb = client.open(addr, 0, "mydb");
        std::string code = 
            "$zCol = 'users';"
            "if( db_exists($zCol) ){"
                "print \"Collection users already created\\n\";"
            "}else{"
                "$rc = db_create($zCol);"
                "if ( !$rc ){"
                    "return;"
                "}"
                "print \"Collection users successfully created\\n\";"
            "}";
        std::unordered_set<std::string> vars;
        std::unordered_map<std::string,std::string> results;
        vars.insert("rc");
        CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                "this call of execute should not throw.",
                mydb.execute(code, vars, &results));
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "rc should be true.",
                std::string("true"), results["rc"]);

        // Assert that the collection the code above created is accessible
        CPPUNIT_ASSERT_EQUAL_MESSAGE(
                "collection users should exist.",
                true, mydb.exists("users"));

        // Test with an erroneous code
        code = "sdasd{}[2";
        CPPUNIT_ASSERT_THROW_MESSAGE(
                "this call of execute should throw.",
                mydb.execute(code, vars, &results),
                sonata::Exception);
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION( ExecTest );
