/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include <cppunit/extensions/HelperMacros.h>
#include <dirent.h>
#include <algorithm>
#include <sonata/Client.hpp>
#include <sonata/Admin.hpp>
#include <spdlog/spdlog.h>

extern thallium::engine* engine;

using namespace std::string_literals;

class Jx9Test : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( Jx9Test );
    CPPUNIT_TEST( testExec );
    CPPUNIT_TEST_SUITE_END();

    static constexpr const char* db_config = "{ \"path\" : \"mydb\" }";

    static std::vector<std::string> list_jx9_files(const std::string& dirname) {
        DIR *dir;
        struct dirent *ent;
        std::vector<std::string> result;
        if((dir = opendir(dirname.c_str())) != nullptr) {
            while((ent = readdir(dir)) != nullptr) {
                result.push_back(dirname+"/"+ent->d_name);
            }
            closedir(dir);
        } else {
            /* could not open directory */
            perror("");
            exit(1);
        }
        auto it = std::remove_if(result.begin(), result.end(), [](const std::string& filename) {
            size_t s = filename.size();
            if(s < 5) return true;
            auto extension = filename.substr(s-4, 4);
            if(extension == ".jx9") return false;
            else return true;
        });
        result.erase(it, result.end());
        std::sort(result.begin(), result.end());
        return result;
    }

    std::vector<std::string> m_jx9_files;

    public:

    void setUp() {}

    void tearDown() {}

    void testExec() {

        m_jx9_files = list_jx9_files(".");
        std::string addr = engine->self();

        for(const auto& jx9_file : m_jx9_files) {

            std::cerr << "JX9 TEST from " << jx9_file << std::endl;
            sonata::Admin admin(*engine);
            admin.createDatabase(addr, 0, "mydb", "unqlite", db_config);

            sonata::Client client(*engine);
            sonata::Database mydb = client.open(addr, 0, "mydb");

            std::ifstream is(jx9_file.c_str());
            std::string code((std::istreambuf_iterator<char>(is)),
                              std::istreambuf_iterator<char>());
            code = "$addr = \""s + addr +"\";\n" + code;

            std::unordered_set<std::string> vars;
            std::unordered_map<std::string,std::string> results;
            vars.insert("rc");
            vars.insert("__output__");
            CPPUNIT_ASSERT_NO_THROW_MESSAGE(
                    "this call to execute should not throw.",
                    mydb.execute(code, vars, &results));
            std::cout << results["__output__"];
            if(jx9_file.find("failing") == std::string::npos) {
                CPPUNIT_ASSERT_EQUAL_MESSAGE(
                        "rc should be true.",
                        std::string("true"), results["rc"]);
            } else {
                CPPUNIT_ASSERT_EQUAL_MESSAGE(
                        "rc should be false.",
                        std::string("false"), results["rc"]);
            }

            admin.destroyDatabase(addr, 0, "mydb");
        }        
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION( Jx9Test );
