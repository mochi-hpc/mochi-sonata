/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "sonata/Provider.hpp"
#include "ProviderImpl.hpp"
#include "sonata/Exception.hpp"

#include <thallium/serialization/stl/string.hpp>

namespace tl = thallium;

namespace {
void delete_engine(void *uargs) { delete static_cast<tl::engine *>(uargs); }
} // namespace

namespace sonata {

Json::Value parseAndValidateJsonConfig(const std::string &config) {
  if (config.empty()) {
    auto result = Json::Value(Json::objectValue);
    result["databases"] = Json::Value(Json::arrayValue);
    return result;
  }
  Json::CharReaderBuilder builder;
  Json::CharReader *reader = builder.newCharReader();

  Json::Value json_config;
  std::string errors;

  bool parsingSuccessful = reader->parse(
      config.c_str(), config.c_str() + config.size(), &json_config, &errors);
  delete reader;

  if (!parsingSuccessful) {
    throw Exception("Could not parse JSON configuration for provider: " +
                    errors);
  }

  if (!json_config.isMember("databases")) {
    json_config["databases"] = Json::Value(Json::arrayValue);
  }

  auto &databases = json_config["databases"];
  if (!databases.isArray()) {
    throw Exception(
        "\"databases\" field in JSON configuration should be an array");
  }

  for (auto &db : databases) {

    if (!db.isMember("name"))
      throw Exception("\"name\" field missing from database configuration");
    if (!db["name"].isString())
      throw Exception(
          "\"name\" field in database configuration should be a string");
    if (db["name"].asString().size() == 0)
      throw Exception("Empty \"name\" field in database configuration");

    if (!db.isMember("type"))
      throw Exception("\"type\" field missing from database configuration");
    if (!db["type"].isString())
      throw Exception(
          "\"type\" field in database configuration should be a string");
    if (db["type"].asString().size() == 0)
      throw Exception("Empty \"type\" field in database configuration");

    if (!db.isMember("mode"))
      throw Exception("\"mode\" field missing from database configuration");
    if (!db["mode"].isString())
      throw Exception(
          "\"mode\" field in database configuration should be a string");
    if (db["mode"].asString() != "attach" && db["mode"].asString() != "create")
      throw Exception("Empty \"mode\" field in database configuration"
                      " should be either \"create\" or \"attach\"");

    if (!db.isMember("config"))
      db["config"] = Json::Value(Json::objectValue);

    if (!db["config"].isObject()) {
      throw Exception("\"config\" field in database configuration "
                      "should be an object");
    }
  }

  return json_config;
}

void populateDatabasesFromConfig(
    const std::shared_ptr<ProviderImpl> &provider_impl,
    const Json::Value &json_config) {
  for (auto &db : json_config["databases"]) {
    auto db_type = db["type"].asString();
    auto db_name = db["name"].asString();
    auto db_config = db["config"];
    std::unique_ptr<Backend> backend;
    if (db["mode"].asString() == "create") {
      backend =
          BackendFactory::createBackend(db_type, provider_impl->get_engine(),
                                        provider_impl->m_pool, db_config);
    } else {
      backend =
          BackendFactory::attachBackend(db_type, provider_impl->get_engine(),
                                        provider_impl->m_pool, db_config);
    }
    spdlog::trace("Added database {} of type {} from JSON configuration",
                  db_name, db_type);
    provider_impl->m_backends[db_name] = std::move(backend);
  }
}

Provider::Provider(tl::engine &engine, uint16_t provider_id,
                   const std::string &config, const tl::pool &p)
    : self(std::make_shared<ProviderImpl>(engine, provider_id, p)) {
  auto json_config = parseAndValidateJsonConfig(config);
  populateDatabasesFromConfig(self, json_config);
  engine.push_finalize_callback(this, [p = this]() { p->self.reset(); });
}

Provider::Provider(margo_instance_id mid, uint16_t provider_id,
                   const std::string &config, const tl::pool &p) {
  auto engine = new tl::engine(mid);
  margo_push_finalize_callback(mid, delete_engine, static_cast<void *>(engine));
  self = std::make_shared<ProviderImpl>(*engine, provider_id, p);
  auto json_config = parseAndValidateJsonConfig(config);
  populateDatabasesFromConfig(self, json_config);
  engine->push_finalize_callback(this, [p = this]() { p->self.reset(); });
}

Provider::Provider(Provider &&other) {
  other.self->get_engine().pop_finalize_callback(this);
  self = std::move(other.self);
  self->get_engine().push_finalize_callback(this,
                                            [p = this]() { p->self.reset(); });
}

Provider::~Provider() {
  if (self) {
    self->get_engine().pop_finalize_callback(this);
  }
}

Provider::operator bool() const { return static_cast<bool>(self); }

std::string Provider::getConfig() const {
  // TODO
  return std::string();
}

void Provider::setSecurityToken(const std::string &token) {
  if (self)
    self->m_token = token;
}

} // namespace sonata
