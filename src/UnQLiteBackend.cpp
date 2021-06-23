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

static UnQLiteBackend::MutexMode getMutexMode(const std::string& mode) {
    if(mode == "global") return UnQLiteBackend::MutexMode::global;
    if(mode == "none")   return UnQLiteBackend::MutexMode::none;
    if(mode == "posix")  return UnQLiteBackend::MutexMode::posix;
    if(mode == "abt")    return UnQLiteBackend::MutexMode::abt;
    return UnQLiteBackend::MutexMode::global;
}

static bool unqlite_lib_is_initialized = false;
static std::string unqlite_mutex_mode = "none";

std::unique_ptr<Backend> UnQLiteBackend::create(const tl::engine &engine,
                                                const tl::pool &pool,
                                                const json &config) {
  bool temporary = config.value("temporary", false);
  bool inmemory = config.value("in-memory", false);
  std::string mutex_mode = config.value("mutex", unqlite_mutex_mode);
  if(mutex_mode != "none"
  && mutex_mode != "global"
  && mutex_mode != "posix"
  && mutex_mode != "abt") {
    throw Exception("\"mutex\" should be either \"none\", "
            "\"global\", \"posix\", or \"abt\"");
  }
  if(unqlite_lib_is_initialized &&
    mutex_mode != unqlite_mutex_mode) {
        throw Exception("All the unqlite databases must have the same "
                "\"mutex\" field");
  }
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
  if (not unqlite_lib_is_initialized) {
    if(mutex_mode == "posix" || mutex_mode == "abt") {
        auto rc = jx9_lib_config(JX9_LIB_CONFIG_THREAD_LEVEL_MULTI);
        if(mutex_mode == "abt")
            jx9_lib_config(JX9_LIB_CONFIG_USER_MUTEX,
                   ExportUnqliteArgobotsMutexMethods());
        unqlite_lib_config(UNQLITE_LIB_CONFIG_THREAD_LEVEL_MULTI);
        if(mutex_mode == "abt")
            unqlite_lib_config(UNQLITE_LIB_CONFIG_USER_MUTEX,
                ExportUnqliteArgobotsMutexMethods());
        unqlite_lib_init();
        auto threadsafe = unqlite_lib_is_threadsafe();
        if(!threadsafe) {
            throw Exception("Requested \""s + mutex_mode
                + "\" mutex but Unqlite is not threadsafe");
        }
    } else {
        jx9_lib_config(JX9_LIB_CONFIG_THREAD_LEVEL_SINGLE);
        unqlite_lib_config(UNQLITE_LIB_CONFIG_THREAD_LEVEL_SINGLE);
        unqlite_lib_init();
    }
    unqlite_lib_is_initialized = true;
    unqlite_mutex_mode = mutex_mode;
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
  backend->m_mutex_mode = getMutexMode(unqlite_mutex_mode);
  spdlog::trace("[unqlite] Successfully created database at {}", db_path);
  return backend;
}

std::unique_ptr<Backend> UnQLiteBackend::attach(const tl::engine &engine,
                                                const tl::pool &pool,
                                                const json &config) {
  std::string mutex_mode = config.value("mutex", unqlite_mutex_mode);
  if(mutex_mode != "none"
  && mutex_mode != "global"
  && mutex_mode != "posix"
  && mutex_mode != "abt") {
    throw Exception("\"mutex\" should be either \"none\", "
            "\"global\", \"posix\", or \"abt\"");
  }
  if(unqlite_lib_is_initialized &&
    mutex_mode != unqlite_mutex_mode) {
        throw Exception("All the unqlite databases must have the same "
                "\"mutex\" field");
  }
  if (not config.contains("path"))
    throw Exception("UnQLiteBackend needs to be initialized with a path");
  std::string db_path = config["path"].get<std::string>();
  std::ifstream f(db_path.c_str());
  if (!f.good()) {
    throw Exception("Database file "s + db_path + " does not exist");
  }
  if (not unqlite_lib_is_initialized) {
    if(mutex_mode == "posix" || mutex_mode == "abt") {
        jx9_lib_config(JX9_LIB_CONFIG_THREAD_LEVEL_MULTI);
        if(mutex_mode == "abt")
            jx9_lib_config(JX9_LIB_CONFIG_USER_MUTEX,
                   ExportUnqliteArgobotsMutexMethods());
        unqlite_lib_config(UNQLITE_LIB_CONFIG_THREAD_LEVEL_MULTI);
        if(mutex_mode == "abt")
            unqlite_lib_config(UNQLITE_LIB_CONFIG_USER_MUTEX,
                ExportUnqliteArgobotsMutexMethods());
        unqlite_lib_init();
        auto threadsafe = unqlite_lib_is_threadsafe();
        if(!threadsafe) {
            throw Exception("Requested \""s + mutex_mode
                + "\" mutex but Unqlite is not threadsafe");
        }
    } else {
        jx9_lib_config(JX9_LIB_CONFIG_THREAD_LEVEL_SINGLE);
        unqlite_lib_config(UNQLITE_LIB_CONFIG_THREAD_LEVEL_SINGLE);
        unqlite_lib_init();
    }
    unqlite_lib_is_initialized = true;
    unqlite_mutex_mode = mutex_mode;
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
  backend->m_mutex_mode = getMutexMode(unqlite_mutex_mode);
  spdlog::trace("[unqlite] Successfully opened database at {}", db_path);
  return backend;
}

} // namespace sonata
