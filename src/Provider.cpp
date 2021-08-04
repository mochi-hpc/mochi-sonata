/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "sonata/Provider.hpp"
#include "ProviderImpl.hpp"
#include "sonata/Exception.hpp"
#include <nlohmann/json.hpp>

#include <thallium/serialization/stl/string.hpp>

namespace tl = thallium;
using nlohmann::json;

namespace {
void delete_engine(void *uargs) { delete static_cast<tl::engine *>(uargs); }
} // namespace

namespace sonata {

json parseAndValidateJsonConfig(const std::string &config) {
  if (config.empty()) {
    auto result = json::object();
    result["databases"] = json::array();
    return result;
  }

  json json_config;
  try {
    json_config = json::parse(config);
  } catch(const std::exception& ex) {
    throw Exception("Could not parse JSON configuration for provider: "s +
                    ex.what());
  }

  if (!json_config.contains("databases")) {
    json_config["databases"] = json::array();
  }

  auto &databases = json_config["databases"];
  if (!databases.is_array()) {
    throw Exception(
        "\"databases\" field in JSON configuration should be an array");
  }

  for (auto &db : databases) {

    if (!db.contains("name"))
      throw Exception("\"name\" field missing from database configuration");
    if (!db["name"].is_string())
      throw Exception(
          "\"name\" field in database configuration should be a string");
    if (db["name"].get_ref<const std::string&>().empty())
      throw Exception("Empty \"name\" field in database configuration");

    if (!db.contains("type"))
      throw Exception("\"type\" field missing from database configuration");
    if (!db["type"].is_string())
      throw Exception(
          "\"type\" field in database configuration should be a string");
    if (db["type"].get_ref<const std::string&>().empty())
      throw Exception("Empty \"type\" field in database configuration");

    if (!db.contains("mode"))
      throw Exception("\"mode\" field missing from database configuration");
    if (!db["mode"].is_string())
      throw Exception(
          "\"mode\" field in database configuration should be a string");
    if (db["mode"].get_ref<const std::string&>() != "attach"
    &&  db["mode"].get_ref<const std::string&>() != "create")
      throw Exception("\"mode\" field in database configuration"
                      " should be either \"create\" or \"attach\"");

    if (!db.contains("config"))
      db["config"] = json::object();

    if (!db["config"].is_object()) {
      throw Exception("\"config\" field in database configuration "
                      "should be an object");
    }
  }

  return json_config;
}

void populateDatabasesFromConfig(
    const std::shared_ptr<ProviderImpl> &provider_impl,
    const json &json_config) {
  for (auto &db : json_config["databases"]) {
    auto& db_type = db["type"].get_ref<const std::string&>();
    auto& db_name = db["name"].get_ref<const std::string&>();
    auto& db_config = db["config"];
    std::unique_ptr<Backend> backend;
    if (db["mode"].get_ref<const std::string&>() == "create") {
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
    provider_impl->m_backend_types[db_name] = db_type;
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
  other.self->get_engine().pop_finalize_callback(&other);
  self = std::move(other.self);
  other.self.reset();
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
  if (!self)
    return "{}";
  std::stringstream ss;
  ss << "{ \"databases\": [";
  std::lock_guard<tl::mutex> g(self->m_backends_mtx);
  for (auto &p : self->m_backends) {
    auto db_name = p.first;
    auto db_type = self->m_backend_types[db_name];
    auto db_config = self->m_backends[db_name]->getConfig();
    ss << "{\"name\": \"" << db_name << "\", "
       << "\"type\": \"" << db_type << "\", "
       << "\"mode\": \"attach\", "
       << "\"config\": " << db_config << "}";
  }
  ss << "]}";
  return ss.str();
}

void Provider::setSecurityToken(const std::string &token) {
  if (self)
    self->m_token = token;
}

} // namespace sonata
