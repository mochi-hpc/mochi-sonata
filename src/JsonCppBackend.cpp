/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "JsonCppBackend.hpp"

namespace sonata {

namespace tl = thallium;
using namespace std::string_literals;

SONATA_REGISTER_BACKEND(jsoncpp, JsonCppBackend);

std::unique_ptr<Backend> JsonCppBackend::create(const thallium::engine &engine,
                                                const tl::pool &pool,
                                                const Json::Value &config) {
  /*
  bool inmemory  = config.get("in-memory", false).asBool();
  if((not config.isMember("path")) && not inmemory)
      throw Exception("JsonCppBackend needs to be initialized with a path");
  std::string db_path = config.get("path","").asString();
  if(db_path.size() > 0) {
      std::ifstream f(db_path.c_str());
      if(f.good()) {
          throw Exception("Database file "s + db_path + " already exists");
      }
  }
  */
  spdlog::trace("[jsoncpp] Creating JsonCpp database");
  int ret;
  auto backend = std::make_unique<JsonCppBackend>();
  spdlog::trace("[jsoncpp] Successfully created database");
  return backend;
}

std::unique_ptr<Backend> JsonCppBackend::attach(const thallium::engine &engine,
                                                const tl::pool &pool,
                                                const Json::Value &config) {
  /*
  if(not config.isMember("path"))
      throw Exception("JsonCppBackend needs to be initialized with a path");
  std::string db_path = config["path"].asString();
  std::ifstream f(db_path.c_str());
  if(!f.good()) {
      throw Exception("Database file "s + db_path + " does not exist");
  }
  */
  spdlog::trace("[jsoncpp] Opening JsonCpp database");
  int ret;
  auto backend = std::make_unique<JsonCppBackend>();
  spdlog::trace("[jsoncpp] Successfully opened database");
  return backend;
}

} // namespace sonata
