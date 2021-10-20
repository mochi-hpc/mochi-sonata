#include <algorithm>
#include <cmath>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mpi.h>
#include <numeric>
#include <random>
#include <sonata/Admin.hpp>
#include <sonata/Client.hpp>
#include <sonata/Provider.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

namespace tl = thallium;
namespace snt = sonata;

using nlohmann::json;
using namespace std::string_literals;

static std::string gen_random_string(size_t len) {
  static const char alphanum[] = "0123456789"
                                 "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                 "abcdefghijklmnopqrstuvwxyz";
  std::string s(len, ' ');
  for (unsigned i = 0; i < len; ++i) {
    s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
  }
  return s;
}

struct RecordInfo {

  size_t num = 0;
  size_t fields = 0;
  size_t min_key_size = 0;
  size_t max_key_size = 0;
  size_t min_val_size = 0;
  size_t max_val_size = 0;
  std::uniform_real_distribution<double> unif =
      std::uniform_real_distribution<double>(0, 1);
  std::default_random_engine rng;

  RecordInfo(const json &config) {
    num = config.value("num", 0);
    fields = config.value("fields", 0);
    if (config.contains("key-size")) {
      if (config["key-size"].is_array() && config["key-size"].size() == 2) {
        min_key_size = config["key-size"][0].get<uint64_t>();
        max_key_size = config["key-size"][1].get<uint64_t>();
      } else if (config["key-size"].is_number()) {
        min_key_size = config["key-size"].get<uint64_t>();
        max_key_size = min_key_size;
      } else {
        throw std::runtime_error("invalid key-size field");
      }
    }
    if (min_key_size == 0 || min_key_size > max_key_size) {
      throw std::runtime_error("invalid key-size value(s)");
    }
    if (config.contains("val-size")) {
      if (config["val-size"].is_array() && config["val-size"].size() == 2) {
        min_val_size = config["val-size"][0].get<uint64_t>();
        max_val_size = config["val-size"][1].get<uint64_t>();
      } else if (config["val-size"].is_number()) {
        min_val_size = config["val-size"].get<uint64_t>();
        max_val_size = min_key_size;
      } else {
        throw std::runtime_error("invalid val-size field");
      }
      if (min_val_size == 0 || min_val_size > max_val_size) {
        throw std::runtime_error("invalid val-size value(s)");
      }
    }
  }

  void generateRandomRecordString(std::string *result) {
    std::stringstream ss;
    ss << "{ ";
    for (size_t i = 0; i < fields; i++) {
      size_t ks = min_key_size + (rand() % (max_key_size - min_key_size + 1));
      std::string key = gen_random_string(ks);
      size_t vs = min_val_size + (rand() % (max_val_size - min_val_size + 1));
      std::string val = gen_random_string(vs);
      ss << "\"" << key << "\" : \"" << val << "\"";
      ss << ", ";
    }

    ss << "\"__p__\" : " << std::setprecision(12) << unif(rng);
    ss << " }";
    *result = ss.str();
  }

  void generateRandomRecordString(json *result) {
    json record;
    for (size_t i = 0; i < fields; i++) {
      size_t ks = min_key_size + (rand() % (max_key_size - min_key_size + 1));
      std::string key = gen_random_string(ks);
      size_t vs = min_val_size + (rand() % (max_val_size - min_val_size + 1));
      std::string val = gen_random_string(vs);
      record[key] = val;
    }
    *result = record;
  }
};

struct CollectionInfo {

  std::string type;
  std::string config;
  std::string database_name;
  std::string collection_name;
  bool keep_db = false;
  bool shared_db = true;

  CollectionInfo(const json &cfg) {
    type = cfg.value("type", "unqlite");
    config = cfg.value("config", json::object()).dump();
    database_name = cfg.value("database-name", "");
    collection_name = cfg.value("collection-name", "");
    shared_db = cfg.value("shared-database", true);
    keep_db = cfg.value("keep-database", false);
    if (database_name.size() == 0) {
      throw std::runtime_error("invalid database name");
    }
    if (collection_name.size() == 0) {
      throw std::runtime_error("invalid collection name");
    }
  }

  snt::Collection createDatabaseAndCollection(MPI_Comm team_comm,
                                              snt::Client &client,
                                              snt::Admin &admin,
                                              const std::string &address,
                                              int team) {
    int rank;
    MPI_Comm_rank(team_comm, &rank);
    if (shared_db) {
      if (rank == 0) {
        auto db_config = json::parse(config);
        if(db_config.contains("path"))
          db_config["path"] = db_config["path"].get<std::string>() + "-" + std::to_string(team);
        admin.createDatabase(address, 0, database_name, type, db_config.dump());
        snt::Database db = client.open(address, 0, database_name);
        snt::Collection coll = db.create(collection_name);
        MPI_Barrier(team_comm);
        return coll;
      } else {
        MPI_Barrier(team_comm);
        snt::Database db = client.open(address, 0, database_name);
        return db.open(collection_name);
      }
    } else {
      auto db_config = json::parse(config);
      if(db_config.contains("path"))
        db_config["path"] = db_config["path"].get<std::string>() + "-" + std::to_string(team)
                       + "-" + std::to_string(rank);
      auto db_name = database_name + "." + std::to_string(rank);
      admin.createDatabase(address, 0, db_name, type, db_config.dump());
      snt::Database db = client.open(address, 0, db_name);
      return db.create(collection_name);
    }
  }

  void eraseDatabaseAndCollection(MPI_Comm team_comm, snt::Client &client,
                                  snt::Admin &admin,
                                  const std::string &address) {
    int rank;
    MPI_Comm_rank(team_comm, &rank);
    if (keep_db)
      return;
    if (shared_db) {
      if (rank == 0)
        admin.destroyDatabase(address, 0, database_name);
    } else {
      admin.destroyDatabase(address, 0,
                            database_name + "." + std::to_string(rank));
    }
  }
};

template <typename T> class BenchmarkRegistration;

/**
 * The AbstractBenchmark class describes an interface that a benchmark object
 * needs to satisfy. This interface has a setup, execute, and teardown
 * methods. AbstractBenchmark also acts as a factory to create concrete
 * instances.
 */
class AbstractBenchmark {

  MPI_Comm m_client_comm;    // communicator gathering all clients
  MPI_Comm m_team_comm;      // communicator gather clients of a team
  std::string m_server_addr; // server address
  snt::Client m_client;      // Sonata client
  snt::Admin m_admin;        // Sonata admin
  int m_team;                // team number

  template <typename T> friend class BenchmarkRegistration;

  using benchmark_factory_function =
      std::function<std::unique_ptr<AbstractBenchmark>(
          json &, MPI_Comm team_comm, MPI_Comm client_comm,
          const std::string &addr, const snt::Client &, const snt::Admin &,
          int team)>;
  static std::map<std::string, benchmark_factory_function>
      s_benchmark_factories;

protected:
  snt::Client &client() { return m_client; }
  snt::Admin &admin() { return m_admin; }
  MPI_Comm client_comm() const { return m_client_comm; }
  MPI_Comm team_comm() const { return m_team_comm; }
  const std::string &server_addr() const { return m_server_addr; }
  int team() const { return m_team; }

public:
  AbstractBenchmark(MPI_Comm team_comm, MPI_Comm client_comm,
                    const std::string &addr, const snt::Client &client,
                    const snt::Admin &admin, int team)
      : m_team_comm(team_comm), m_client_comm(client_comm), m_server_addr(addr),
        m_client(client), m_admin(admin), m_team(team) {}

  virtual ~AbstractBenchmark() = default;
  virtual void setup() = 0;
  virtual void execute() = 0;
  virtual void teardown() = 0;

  /**
   * @brief Factory function used to create benchmark instances.
   */
  template <typename... T>
  static std::unique_ptr<AbstractBenchmark> create(const std::string &type,
                                                   T &&...args) {
    auto it = s_benchmark_factories.find(type);
    if (it == s_benchmark_factories.end())
      throw std::invalid_argument(type + " benchmark type unknown");
    return (it->second)(std::forward<T>(args)...);
  }
};

/**
 * @brief The mechanism bellow is used to provide the REGISTER_BENCHMARK macro,
 * which registers a child class of AbstractBenchmark and allows
 * AbstractBenchmark::create("type", ...) to return an instance of this concrete
 * child class.
 */
template <typename T> class BenchmarkRegistration {
public:
  BenchmarkRegistration(const std::string &type) {
    AbstractBenchmark::s_benchmark_factories[type] =
        [](json &config, MPI_Comm team_comm, MPI_Comm client_comm,
           const std::string &addr, const snt::Client &client,
           const snt::Admin &admin, int team) {
          return std::make_unique<T>(config, team_comm, client_comm, addr,
                                     client, admin, team);
        };
  }
};

std::map<std::string, AbstractBenchmark::benchmark_factory_function>
    AbstractBenchmark::s_benchmark_factories;

#define REGISTER_BENCHMARK(__name, __class)                                    \
  static BenchmarkRegistration<__class> __class##_registration(__name)

/**
 * StoreBenchmark executes a series of store operations and measures their
 * duration.
 */
class StoreBenchmark : public AbstractBenchmark {

protected:
  RecordInfo m_record_info;
  CollectionInfo m_collection_info;
  snt::Collection m_collection;
  bool m_use_json = false;
  std::vector<std::string> m_records;
  std::vector<json> m_records_json;

public:
  template <typename... T>
  StoreBenchmark(json &config, T &&...args)
      : AbstractBenchmark(std::forward<T>(args)...),
        m_record_info(config["records"]),
        m_collection_info(config["collection"]) {
    m_use_json = config.value("use-json", false);
  }

  virtual void setup() override {
    m_collection = m_collection_info.createDatabaseAndCollection(
        team_comm(), client(), admin(), server_addr(), team());

    if (!m_use_json) {
      m_records.reserve(m_record_info.num);
      for (size_t i = 0; i < m_record_info.num; i++) {
        std::string r;
        m_record_info.generateRandomRecordString(&r);
        m_records.push_back(std::move(r));
      }
    } else {
      m_records_json.reserve(m_record_info.num);
      for (size_t i = 0; i < m_record_info.num; i++) {
        json r;
        m_record_info.generateRandomRecordString(&r);
        m_records_json.push_back(std::move(r));
      }
    }
  }

  virtual void execute() override {
    if (!m_use_json) {
      for (auto &r : m_records) {
        m_collection.store(r);
      }
    } else {
      for (auto &r : m_records_json) {
        m_collection.store(r);
      }
    }
  }

  virtual void teardown() override {
    m_collection_info.eraseDatabaseAndCollection(team_comm(), client(), admin(),
                                                 server_addr());
    m_records.clear();
    m_records_json.clear();
  }
};
REGISTER_BENCHMARK("store", StoreBenchmark);

/**
 * StoreMultiBenchmark executes a series of store_multi operations and measures
 * their duration.
 */
class StoreMultiBenchmark : public StoreBenchmark {

protected:
  size_t m_batch_size;
  std::vector<std::vector<std::string>> m_batches;
  std::vector<json> m_batches_json;

public:
  template <typename... T>
  StoreMultiBenchmark(json &config, T &&...args)
      : StoreBenchmark(config, std::forward<T>(args)...),
        m_batch_size(config.value("batch-size", (uint64_t)1)) {}

  virtual void setup() override {
    StoreBenchmark::setup();
    if (!m_use_json) {
      if (m_record_info.num % m_batch_size == 0)
        m_batches.resize(m_record_info.num / m_batch_size);
      else
        m_batches.resize(1 + m_record_info.num / m_batch_size);
      for (unsigned i = 0; i < m_records.size(); i++) {
        m_batches[i / m_batch_size].emplace_back(std::move(m_records[i]));
      }
      m_records.clear();
    } else {
      if (m_record_info.num % m_batch_size == 0)
        m_batches_json.resize(m_record_info.num / m_batch_size);
      else
        m_batches_json.resize(1 + m_record_info.num / m_batch_size);
      for (unsigned i = 0; i < m_records_json.size(); i++) {
        m_batches_json[i / m_batch_size].push_back(std::move(m_records_json[i]));
      }
      m_records_json.clear();
    }
  }

  virtual void execute() override {
    if (!m_use_json) {
      for (auto &records : m_batches) {
        m_collection.store_multi(records, nullptr);
      }
    } else {
      for (auto &records : m_batches_json) {
        m_collection.store_multi(records, nullptr);
      }
    }
  }

  virtual void teardown() override {
    m_batches.clear();
    m_batches_json.clear();
    StoreBenchmark::teardown();
  }
};
REGISTER_BENCHMARK("store-multi", StoreMultiBenchmark);

/**
 * IngestBenchmark stores records taken from one or multiple files.
 */
class IngestBenchmark : public AbstractBenchmark {

protected:
  CollectionInfo m_collection_info;
  snt::Collection m_collection;
  bool m_use_json = false;
  size_t m_batch_size;
  std::vector<std::string> m_input_files;
  std::vector<std::string> m_object_path;
  std::vector<std::vector<std::string>> m_records;
  std::vector<json> m_records_json;
  json &m_config;

public:
  template <typename... T>
  IngestBenchmark(json &config, T &&...args)
      : AbstractBenchmark(std::forward<T>(args)...),
        m_collection_info(config["collection"]),
        m_use_json(config.value("use-json", false)),
        m_batch_size(config.value("batch-size", (uint64_t)1)), m_config(config) {
    auto input_files_json = config.value("files", json());
    if (input_files_json.is_string()) {
      m_input_files.push_back(input_files_json.get<std::string>());
    } else if (input_files_json.is_array()) {
      for (size_t i = 0; i < input_files_json.size(); i++) {
        m_input_files.push_back(
            input_files_json[i].get<std::string>());
      }
    }
    std::string obj = config.value("object-path", "");
    std::stringstream ss(obj);
    std::string token;
    while (std::getline(ss, token, '.')) {
      m_object_path.push_back(token);
    }
  }

  virtual void setup() override {
    spdlog::trace("Setting up IngestBenchmark...");
    m_collection = m_collection_info.createDatabaseAndCollection(
        team_comm(), client(), admin(), server_addr(), team());
    size_t num_objects = 0;
    for (auto &f : m_input_files) {
      std::ifstream file(f);
      if (!file.good())
        throw std::runtime_error("Could not open file "s + f);
      json root;
      file >> root;
      json obj = root;
      size_t current_batch_size = 0;
      if (m_use_json)
        m_records_json.emplace_back();
      else
        m_records.emplace_back();
      for (auto &token : m_object_path) {
        if (obj.contains(token))
          obj = obj[token];
        else
          throw std::runtime_error("Could not find field \""s + token + "\"");
      }
      if (!obj.is_array()) {
        if (m_use_json)
          m_records_json.back().push_back(obj);
        else
          m_records.back().push_back(obj.dump());
        current_batch_size += 1;
        num_objects += 1;
      } else {
        for (size_t i = 0; i < obj.size(); i++) {
          if (m_use_json)
            m_records_json.back().push_back(obj[i]);
          else
            m_records.back().push_back(
                obj[i].dump());
          current_batch_size += 1;
          if (current_batch_size == m_batch_size) {
            current_batch_size = 0;
            if (m_use_json)
              m_records_json.emplace_back();
            else
              m_records.emplace_back();
          }
        }
        num_objects += obj.size();
      }
      if (current_batch_size == m_batch_size) {
        current_batch_size = 0;
        if (m_use_json)
          m_records_json.emplace_back();
        else
          m_records.emplace_back();
      }
    }
    if (m_use_json) {
      spdlog::trace("{} batches of records will be ingested",
                    m_records_json.size());
    } else {
      spdlog::trace("{} batches of records will be ingested", m_records.size());
    }
    m_config["num-records"] = num_objects;
  }

  virtual void execute() override {
    spdlog::trace("Executing IngestBechmark...");
    if (m_use_json) {
      for (size_t i = 0; i < m_records_json.size(); i++) {
        m_collection.store_multi(m_records_json[i], nullptr);
      }
    } else {
      for (size_t i = 0; i < m_records.size(); i++)
        m_collection.store_multi(m_records[i], nullptr);
    }
    spdlog::trace("IngestBenchmark executed");
  }

  virtual void teardown() override {
    spdlog::trace("Tearing down IngestBenchmark...");
    m_collection_info.eraseDatabaseAndCollection(team_comm(), client(), admin(),
                                                 server_addr());
    m_records.clear();
    m_records_json.clear();
    spdlog::trace("Done tearing down IngestBenchmark");
  }
};
REGISTER_BENCHMARK("ingest", IngestBenchmark);

/**
 * FetchBenchmark executes a series of fetch operations and measures their
 * duration.
 */
class FetchBenchmark : public AbstractBenchmark {

protected:
  RecordInfo m_record_info;
  CollectionInfo m_collection_info;
  snt::Collection m_collection;
  bool m_use_json = false;
  size_t m_num_fetch = 0;

public:
  template <typename... T>
  FetchBenchmark(json &config, T &&...args)
      : AbstractBenchmark(std::forward<T>(args)...),
        m_record_info(config["records"]),
        m_collection_info(config["collection"]) {
    m_use_json = config.value("use-json", false);
    if (!config.contains("num-operations")) {
      throw std::runtime_error("Benchmark needs a num-operations parameter");
    }
    m_num_fetch = config["num-operations"].get<uint64_t>();
  }

  virtual void setup() override {
    int rank;
    MPI_Comm_rank(team_comm(), &rank);
    m_collection = m_collection_info.createDatabaseAndCollection(
        team_comm(), client(), admin(), server_addr(), team());
    if (rank == 0 || !m_collection_info.shared_db) {
      for (size_t i = 0; i < m_record_info.num; i++) {
        std::string r;
        m_record_info.generateRandomRecordString(&r);
        m_collection.store(r, i == m_record_info.num-1);
      }
    }
  }

  virtual void execute() override {
    for (size_t i = 0; i < m_num_fetch; i++) {
      uint64_t id = rand() % m_record_info.num;
      if (!m_use_json) {
        std::string r;
        m_collection.fetch(id, &r);
      } else {
        json r;
        m_collection.fetch(id, &r);
      }
    }
  }

  virtual void teardown() override {
    m_collection_info.eraseDatabaseAndCollection(team_comm(), client(), admin(),
                                                 server_addr());
  }
};
REGISTER_BENCHMARK("fetch", FetchBenchmark);

/**
 * FetchMultiBenchmark executes a series of fetch-multi operations and measures
 * their duration.
 */
class FetchMultiBenchmark : public FetchBenchmark {

protected:
  size_t m_batch_size;

public:
  template <typename... T>
  FetchMultiBenchmark(json &config, T &&...args)
      : FetchBenchmark(config, std::forward<T>(args)...),
        m_batch_size(config.value("batch-size", (uint64_t)1)) {}

  virtual void execute() override {
    std::vector<uint64_t> ids(m_batch_size);
    for (size_t i = 0; i < m_num_fetch; i++) {
      for (size_t j = 0; j < ids.size(); j++) {
        uint64_t id = rand() % m_record_info.num;
        ids[j] = id;
      }
      if (!m_use_json) {
        std::vector<std::string> r;
        m_collection.fetch_multi(ids.data(), ids.size(), &r);
      } else {
        json r;
        m_collection.fetch_multi(ids.data(), ids.size(), &r);
      }
    }
  }
};
REGISTER_BENCHMARK("fetch-multi", FetchMultiBenchmark);

/**
 * FilterBenchmark executes a series of filter operations and measures their
 * duration.
 */
class FilterBenchmark : public FetchBenchmark {

protected:
  double m_selectivity = 0.0;
  std::string m_function;

public:
  template <typename... T>
  FilterBenchmark(json &config, T &&...args)
      : FetchBenchmark(config, std::forward<T>(args)...) {
    m_use_json = config.value("use-json", false);
    if (!config.contains("filter-selectivity")) {
      throw std::runtime_error(
          "Filter benchmark needs a filter-selectivity parameter");
    }
    m_selectivity = config["filter-selectivity"].get<double>();
    std::stringstream ss;
    ss << "function($rec) { return $rec.__p__ < ";
    ss << std::setprecision(12) << m_selectivity;
    ss << "; }";
    m_function = ss.str();
  }

  virtual void execute() override {
    if (!m_use_json) {
      std::vector<std::string> result;
      m_collection.filter(m_function, &result);
    } else {
      json result;
      m_collection.filter(m_function, &result);
    }
  }
};
REGISTER_BENCHMARK("filter", FilterBenchmark);

/**
 * UpdateBenchmark executes a series of update operations and measures their
 * duration.
 */
class UpdateBenchmark : public FetchBenchmark {

protected:
  size_t m_num_updates = 0;
  std::vector<std::string> m_new_records;
  std::vector<json> m_new_records_json;
  std::vector<uint64_t> m_ids_to_update;

public:
  template <typename... T>
  UpdateBenchmark(json &config, T &&...args)
      : FetchBenchmark(config, std::forward<T>(args)...) {
    m_num_updates = config["num-operations"].get<uint64_t>();
  }

  virtual void setup() override {
    FetchBenchmark::setup();

    if (m_use_json)
      m_new_records_json.reserve(m_num_updates);
    else
      m_new_records.reserve(m_num_updates);

    for (size_t i = 0; i < m_num_updates; i++) {
      if (!m_use_json) {
        std::string r;
        m_record_info.generateRandomRecordString(&r);
        m_new_records.push_back(std::move(r));
      } else {
        json r;
        m_record_info.generateRandomRecordString(&r);
        m_new_records_json.push_back(std::move(r));
      }
    }

    for (size_t i = 0; i < m_num_updates; i++) {
      uint64_t id = rand() % m_record_info.num;
      m_ids_to_update.push_back(id);
    }
  }

  virtual void execute() override {
    for (size_t i = 0; i < m_num_updates; i++) {
      uint64_t id = m_ids_to_update[i];
      if (!m_use_json) {
        m_collection.update(id, m_new_records[i]);
      } else {
        m_collection.update(id, m_new_records_json[i]);
      }
    }
  }
};
REGISTER_BENCHMARK("update", UpdateBenchmark);

/**
 * UpdateMultiBenchmark executes a series of update operations and measures
 * their duration.
 */
class UpdateMultiBenchmark : public UpdateBenchmark {

protected:
  size_t m_batch_size;
  std::vector<std::vector<std::string>> m_new_records_batches;
  std::vector<json> m_new_records_batches_json;

public:
  template <typename... T>
  UpdateMultiBenchmark(json &config, T &&...args)
      : UpdateBenchmark(config, std::forward<T>(args)...),
        m_batch_size(config.value("batch-size", (uint64_t)1)) {}

  virtual void setup() override {
    UpdateBenchmark::setup();
    size_t remaining = m_num_updates;
    size_t i = 0;
    while (remaining != 0) {
      size_t batch_size = std::min(m_batch_size, remaining);
      if (!m_use_json) {
        m_new_records_batches.emplace_back();
      } else {
        m_new_records_batches_json.emplace_back();
      }
      for (size_t j = 0; j < batch_size; j++) {
        if (!m_use_json) {
          m_new_records_batches.back().push_back(
              std::move(m_new_records[i + j]));
        } else {
          m_new_records_batches_json.back().push_back(
              std::move(m_new_records_json[i + j]));
        }
      }
      remaining -= batch_size;
      i += batch_size;
    }
    m_new_records.clear();
    m_new_records_json.clear();
  }

  virtual void execute() override {
    if (m_use_json) {
      size_t offset_id = 0;
      for (size_t i = 0; i < m_new_records_batches_json.size(); i++) {
        m_collection.update_multi(&m_ids_to_update[offset_id],
                                  m_new_records_batches_json[i], nullptr);
        offset_id += m_new_records_batches_json[i].size();
      }
    } else {
      size_t offset_id = 0;
      for (size_t i = 0; i < m_new_records_batches.size(); i++) {
        m_collection.update_multi(&m_ids_to_update[offset_id],
                                  m_new_records_batches[i], nullptr);
        offset_id += m_new_records_batches[i].size();
      }
    }
  }

  virtual void teardown() override {
    m_new_records_batches.clear();
    m_new_records_batches_json.clear();
    UpdateBenchmark::teardown();
  }
};
REGISTER_BENCHMARK("update-multi", UpdateMultiBenchmark);
/**
 * AllBenchmark executes a series of "all" operations and measures their
 * duration.
 */
class AllBenchmark : public FetchBenchmark {

public:
  template <typename... T>
  AllBenchmark(json &config, T &&...args)
      : FetchBenchmark(config, std::forward<T>(args)...) {}

  virtual void execute() override {
    if (m_use_json) {
      json all;
      m_collection.all(&all);
    } else {
      std::vector<std::string> all;
      m_collection.all(&all);
    }
  }
};
REGISTER_BENCHMARK("all", AllBenchmark);

/**
 * EraseBenchmark executes a series of "erase" operations and measures their
 * duration.
 */
class EraseBenchmark : public FetchBenchmark {

protected:
  std::vector<uint64_t> m_erase_order;

public:
  template <typename... T>
  EraseBenchmark(json &config, T &&...args)
      : FetchBenchmark(config, std::forward<T>(args)...) {}

  virtual void setup() override {
    FetchBenchmark::setup();
    int rank;
    MPI_Comm_rank(team_comm(), &rank);
    m_erase_order.resize(m_record_info.num);
    for (size_t i = 0; i < m_record_info.num; i++) {
      m_erase_order[i] = i;
    }
    std::random_shuffle(m_erase_order.begin(), m_erase_order.end());
    if (m_collection_info.shared_db) {
      MPI_Bcast(m_erase_order.data(), m_record_info.num * sizeof(uint64_t),
                MPI_BYTE, 0, team_comm());
    }
  }

  virtual void execute() override {
    int rank;
    int size;
    MPI_Comm_rank(team_comm(), &rank);
    MPI_Comm_size(team_comm(), &size);
    for (size_t i = 0; i < m_erase_order.size(); i++) {
      if (m_collection_info.shared_db) {
        if (i % size == rank)
          m_collection.erase(m_erase_order[i]);
      } else {
        m_collection.erase(m_erase_order[i]);
      }
    }
  }
};
REGISTER_BENCHMARK("erase", EraseBenchmark);

/**
 * EraseMultiBenchmark executes a series of "erase-multi" operations and
 * measures their duration.
 */
class EraseMultiBenchmark : public EraseBenchmark {

protected:
  size_t m_batch_size;

public:
  template <typename... T>
  EraseMultiBenchmark(json &config, T &&...args)
      : EraseBenchmark(config, std::forward<T>(args)...),
        m_batch_size(config.value("batch-size", (uint64_t)1)) {}

  virtual void execute() override {
    int rank;
    int size;
    MPI_Comm_rank(team_comm(), &rank);
    MPI_Comm_size(team_comm(), &size);
    size_t chunk_size = m_erase_order.size() / size;
    size_t remaining =
        m_collection_info.shared_db ? chunk_size : m_erase_order.size();
    size_t i = m_collection_info.shared_db ? rank * chunk_size : 0;
    while (remaining != 0) {
      size_t batch_size = std::min(remaining, m_batch_size);
      m_collection.erase_multi(&m_erase_order[i], batch_size);
      remaining -= batch_size;
      i += batch_size;
    }
  }
};
REGISTER_BENCHMARK("erase-multi", EraseMultiBenchmark);

static void run_server(MPI_Comm group_comm, json &config);
static void run_client(MPI_Comm group_comm, MPI_Comm client_comm,
                       json &config, int team, int benchmark_id);

/**
 * @brief Main function.
 */
int main(int argc, char **argv) {

  MPI_Init(&argc, &argv);
  int rank, size;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  if (argc < 2) {
    if (rank == 0) {
      std::cerr << "Usage: " << argv[0] << " <config.json>" << std::endl;
      MPI_Abort(MPI_COMM_WORLD, -1);
    }
  }

  std::ifstream config_file(argv[1]);
  if (!config_file.good() && rank == 0) {
    std::cerr << "Could not read configuration file " << argv[1] << std::endl;
    MPI_Abort(MPI_COMM_WORLD, -1);
  }

  json config = json::parse(config_file);

  int server_count = 1;
  if (config.contains("server") && config["server"].contains("count")) {
    server_count = config["server"]["count"].get<int>();
    if ((size - server_count) % server_count != 0 && rank == 0) {
      std::cerr << "Number of servers does not divide the number of clients"
                << std::endl;
      MPI_Abort(MPI_COMM_WORLD, -1);
    }
  }

  MPI_Comm type_comm; // servers or clients
  MPI_Comm_split(MPI_COMM_WORLD, rank < server_count ? 0 : 1, rank, &type_comm);

  MPI_Comm group_comm; // team of clients + their dedicated server
  int team;
  if (rank < server_count) {
    team = rank;
  } else {
    int client_rank;
    MPI_Comm_rank(type_comm, &client_rank);
    team = client_rank % server_count;
  }
  MPI_Comm_split(MPI_COMM_WORLD, team, rank, &group_comm);

  int benchmark_id = -1;
  if (argc >= 3)
    benchmark_id = atoi(argv[2]);
  if (rank < server_count) {
    run_server(group_comm, config);
  } else {
    run_client(group_comm, type_comm, config, team, benchmark_id);
  }

  MPI_Finalize();
  return 0;
}

static void run_server(MPI_Comm group_comm, json &config) {
  // initialize Thallium
  std::string loglevel = config.value("log", "info");
  spdlog::set_level(spdlog::level::from_str(loglevel));
  std::string protocol = config["protocol"].get<std::string>();
  bool use_progress_thread = false;
  int rpc_thread_count = 0;
  if (config.contains("server")) {
    auto &server_config = config["server"];
    use_progress_thread =
        server_config.value("use-progress-thread", false);
    rpc_thread_count = server_config.value("rpc-thread-count", 0);
  }
  tl::engine engine(protocol, THALLIUM_SERVER_MODE, use_progress_thread,
                    rpc_thread_count);
  engine.enable_remote_shutdown();
  // serialize server address
  std::string server_addr = engine.self();
  // send server address to client
  size_t server_addr_size = server_addr.size();
  MPI_Bcast(&server_addr_size, sizeof(server_addr_size), MPI_BYTE, 0,
            group_comm);
  MPI_Bcast(const_cast<char *>(server_addr.data()), server_addr_size, MPI_BYTE,
            0, group_comm);
  // clients are creating their team_comm (server doesn't need it but need to
  // participate)
  MPI_Comm team_comm;
  MPI_Comm_split(group_comm, 0, 0, &team_comm);
  // initialize Sonata provider
  snt::Provider provider(engine);
  // notify clients that the provider is ready
  MPI_Barrier(MPI_COMM_WORLD);
  // wait for finalize
  engine.wait_for_finalize();
}

static void run_client(MPI_Comm group_comm, MPI_Comm client_comm,
                       json &config, int team, int benchmark_id) {
  std::string loglevel = config.value("log", "info");
  spdlog::set_level(spdlog::level::from_str(loglevel));
  // get info from communicator
  int rank, num_clients;
  MPI_Comm_rank(client_comm, &rank);
  MPI_Comm_size(client_comm, &num_clients);
  // initialize Thallium
  std::string protocol = config["protocol"].get<std::string>();
  bool use_progress_thread = false;
  if (config.contains("client")) {
    auto &client_config = config["client"];
    use_progress_thread =
        client_config.value("use-progress-thread", false);
  }
  tl::engine engine(protocol, THALLIUM_CLIENT_MODE, use_progress_thread, 0);
  // receive server address
  std::string server_addr_str;
  size_t addr_size;
  MPI_Bcast(&addr_size, sizeof(size_t), MPI_BYTE, 0, group_comm);
  server_addr_str.resize(addr_size, '\0');
  MPI_Bcast(const_cast<char *>(server_addr_str.data()), addr_size, MPI_BYTE, 0,
            group_comm);
  // create team_comm
  MPI_Comm team_comm;
  int group_rank;
  MPI_Comm_rank(group_comm, &group_rank);
  MPI_Comm_split(group_comm, 1, group_rank, &team_comm);
  // wait for servers to have initialized
  MPI_Barrier(MPI_COMM_WORLD);
  {
    // open remote database
    snt::Client client(engine);
    snt::Admin admin(engine);
    // initialize the RNG seed
    int seed = config["seed"].get<int>();
    // initialize benchmark instances
    std::vector<std::unique_ptr<AbstractBenchmark>> benchmarks;
    std::vector<unsigned> repetitions;
    std::vector<std::string> types;
    benchmarks.reserve(config["benchmarks"].size());
    repetitions.reserve(config["benchmarks"].size());
    types.reserve(config["benchmarks"].size());
    int current_benchmark = 0;
    for (auto &bench_config : config["benchmarks"]) {
      if (current_benchmark != benchmark_id && benchmark_id >= 0)
        continue;

      std::string type = bench_config["type"].get<std::string>();
      types.push_back(type);
      benchmarks.push_back(
          AbstractBenchmark::create(type, bench_config, team_comm, client_comm,
                                    server_addr_str, client, admin, team));
      repetitions.push_back(bench_config["repetitions"].get<unsigned>());
      current_benchmark += 1;
    }
    // main execution loop
    for (unsigned i = 0; i < benchmarks.size(); i++) {
      auto &bench = benchmarks[i];
      unsigned rep = repetitions[i];
      // reset the RNG
      srand(seed + rank * 1789);
      std::vector<double> local_timings(rep);
      for (unsigned j = 0; j < rep; j++) {
        MPI_Barrier(client_comm);
        // benchmark setup
        bench->setup();
        MPI_Barrier(client_comm);
        // benchmark execution
        double t_start = MPI_Wtime();
        bench->execute();
        double t_end = MPI_Wtime();
        local_timings[j] = t_end - t_start;
        MPI_Barrier(client_comm);
        // teardown
        bench->teardown();
      }
      // exchange timings
      std::vector<double> global_timings(rep * num_clients);
      if (num_clients != 1) {
        MPI_Gather(local_timings.data(), local_timings.size(), MPI_DOUBLE,
                   global_timings.data(), local_timings.size(), MPI_DOUBLE, 0,
                   client_comm);
      } else {
        std::copy(local_timings.begin(), local_timings.end(),
                  global_timings.begin());
      }
      // print report
      if (rank == 0) {
        size_t n = global_timings.size();
        std::cout << "================ " << types[i]
                  << " ================" << std::endl;
        std::cout << config["benchmarks"][i].dump(4);
        std::cout << std::endl;
        std::cout << "-----------------" << std::string(types[i].size(), '-')
                  << "-----------------" << std::endl;
        double average =
            std::accumulate(global_timings.begin(), global_timings.end(), 0.0) /
            n;
        double variance =
            std::accumulate(global_timings.begin(), global_timings.end(), 0.0,
                            [average](double acc, double x) {
                              return acc + std::pow((x - average), 2);
                            });
        variance /= n;
        double stddev = std::sqrt(variance);
        std::sort(global_timings.begin(), global_timings.end());
        double min = global_timings[0];
        double max = global_timings[global_timings.size() - 1];
        double median =
            (n % 2)
                ? global_timings[n / 2]
                : ((global_timings[n / 2] + global_timings[n / 2 - 1]) / 2.0);
        double q1 = global_timings[n / 4];
        double q3 = global_timings[(3 * n) / 4];
        std::cout << std::setprecision(9) << std::fixed;
        std::cout << "Samples         : " << n << std::endl;
        std::cout << "Average(sec)    : " << average << std::endl;
        std::cout << "Variance(sec^2) : " << variance << std::endl;
        std::cout << "StdDev(sec)     : " << stddev << std::endl;
        std::cout << "Minimum(sec)    : " << min << std::endl;
        std::cout << "Q1(sec)         : " << q1 << std::endl;
        std::cout << "Median(sec)     : " << median << std::endl;
        std::cout << "Q3(sec)         : " << q3 << std::endl;
        std::cout << "Maximum(sec)    : " << max << std::endl;
      }
    }
    // wait for all the clients to be done with their tasks
    MPI_Barrier(client_comm);
    // shutdown server and finalize margo
    int team_rank;
    MPI_Comm_rank(team_comm, &team_rank);
    if (team_rank == 0) {
      auto server_addr = engine.lookup(server_addr_str);
      engine.shutdown_remote_engine(server_addr);
    }
  }
}
