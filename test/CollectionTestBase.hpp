/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __COLLECTION_TEST_BASE
#define __COLLECTION_TEST_BASE

#include <string>
#include <vector>
#include <fstream>
#include <streambuf>
#include <nlohmann/json.hpp>

using nlohmann::json;

class CollectionTestBase {

    public:

    CollectionTestBase() {
        std::ifstream t("test.json");
        std::string str((std::istreambuf_iterator<char>(t)),
                         std::istreambuf_iterator<char>());
        json j = json::parse(str);
        for(unsigned i = 0; i < j.size(); i++) {
            records_json.push_back(j[i]);
            records_str.push_back(j[i].dump());
        }
        records_json_all = j;
    }

    std::vector<std::string> records_str;
    std::vector<json> records_json;
    json records_json_all;
};

#endif
