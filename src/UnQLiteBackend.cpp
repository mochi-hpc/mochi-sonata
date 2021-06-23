/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "UnQLiteBackend.hpp"

namespace sonata {

namespace tl = thallium;
using namespace std::string_literals;

SONATA_REGISTER_BACKEND(unqlite, UnQLiteBackend);

std::unique_ptr<Backend> UnQLiteBackend::create(const tl::engine &engine,
                                                const tl::pool &pool,
                                                const json &config) {
  bool temporary = config.value("temporary", false);
  bool inmemory = config.value("in-memory", false);
  if ((not config.contains("path")) && not inmemory)
    throw Exception("UnQLiteBackend needs to be initialized with a path");
  std::string db_path = config.value("path", "");
  if (db_path.size() > 0) {
    std::ifstream f(db_path.c_str());
    if (f.good()) {
      throw Exception("Database file "s + db_path + " already exists");
    }
  }
  // Setup Unqlite so it can be multithreaded
  static bool unqlite_lib_is_initialized = false;
  if (not unqlite_lib_is_initialized) {
    jx9_lib_config(JX9_LIB_CONFIG_THREAD_LEVEL_MULTI);
    jx9_lib_config(JX9_LIB_CONFIG_USER_MUTEX,
                   ExportUnqliteArgobotsMutexMethods());
    unqlite_lib_config(UNQLITE_LIB_CONFIG_THREAD_LEVEL_MULTI);
    unqlite_lib_config(UNQLITE_LIB_CONFIG_USER_MUTEX,
                       ExportUnqliteArgobotsMutexMethods());
    unqlite_lib_is_initialized = true;
  }
  // Open the Unqlite database
  unqlite *pDB;
  spdlog::trace("[unqlite] Creating UnQLite database");
  int ret;
  int mode = UNQLITE_OPEN_CREATE;
  if (inmemory) {
    ret = unqlite_open(&pDB, nullptr, mode);
  } else {
    if (temporary)
      mode = mode | UNQLITE_OPEN_TEMP_DB;
    ret = unqlite_open(&pDB, db_path.c_str(), mode);
    if (ret != UNQLITE_OK) {
      throw Exception("Could not open or create database at "s + db_path);
    }
    // forcing the file to be created
    unqlite_kv_store(pDB, "___", -1, "", 1);
    unqlite_kv_delete(pDB, "___", -1);
  }
  auto backend = std::make_unique<UnQLiteBackend>();
  backend->m_db = pDB;
  backend->m_is_temporary = temporary;
  backend->m_is_in_memory = inmemory;
  backend->m_filename = db_path;
  backend->m_client = Client(engine);
  backend->m_admin = Admin(engine);
  spdlog::trace("[unqlite] Successfully created database at {}", db_path);
  return backend;
}

std::unique_ptr<Backend> UnQLiteBackend::attach(const tl::engine &engine,
                                                const tl::pool &pool,
                                                const json &config) {
  if (not config.contains("path"))
    throw Exception("UnQLiteBackend needs to be initialized with a path");
  std::string db_path = config["path"].get<std::string>();
  std::ifstream f(db_path.c_str());
  if (!f.good()) {
    throw Exception("Database file "s + db_path + " does not exist");
  }
  unqlite *pDB;
  spdlog::trace("[unqlite] Opening UnQLite database");
  int ret;
  int mode = UNQLITE_OPEN_READWRITE;
  ret = unqlite_open(&pDB, db_path.c_str(), mode);
  if (ret != UNQLITE_OK) {
    throw Exception("Could not open database at "s + db_path);
  }
  auto backend = std::make_unique<UnQLiteBackend>();
  backend->m_db = pDB;
  backend->m_is_temporary = false;
  backend->m_is_in_memory = false;
  backend->m_filename = db_path;
  backend->m_client = Client(engine);
  backend->m_admin = Admin(engine);
  spdlog::trace("[unqlite] Successfully opened database at {}", db_path);
  return backend;
}

} // namespace sonata
