#ifndef __COLLECTION_TEST_BASE
#define __COLLECTION_TEST_BASE

#include <string>
#include <vector>
#include <fstream>
#include <streambuf>
#include <json/json.h>

class CollectionTestBase {

    public:

    CollectionTestBase() {
        std::ifstream t("test.json");
        std::string str((std::istreambuf_iterator<char>(t)),
                         std::istreambuf_iterator<char>());
        Json::CharReaderBuilder builder;
        Json::CharReader* reader = builder.newCharReader();
        Json::Value json;
        std::string errors;
        bool parsingSuccessful = reader->parse(
                str.c_str(),
                str.c_str() + str.size(),
                &json,
                &errors
                );
        delete reader;
        if (!parsingSuccessful) {
            throw std::runtime_error("Failed to parse test JSON file");
        }
        for(unsigned i = 0; i < json.size(); i++) {
            records_json.push_back(json[i]);
            records_str.push_back(json[i].toStyledString());
        }
    }

    std::vector<std::string> records_str;
    std::vector<Json::Value> records_json;

};

#endif
