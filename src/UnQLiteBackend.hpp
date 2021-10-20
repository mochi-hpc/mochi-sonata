/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_UNQLITE_BACKEND_HPP
#define __SONATA_UNQLITE_BACKEND_HPP
#include "sonata/Admin.hpp"
#include "sonata/Backend.hpp"
#include "sonata/Client.hpp"
#include "unqlite/jx9.h"
#include "unqlite/unqlite.h"

#include "UnQLiteMutex.hpp"
#include "UnQLiteVM.hpp"
#include "UnQLiteJsonEncoder.hpp"

#include <cstdio>
#include <fstream>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <sstream>
#include <thallium.hpp>

namespace sonata {

namespace tl = thallium;
using namespace std::string_literals;
using nlohmann::json;

class UnQLiteVM;

class UnQLiteBackend : public Backend {

  friend class UnQLiteVM;

public:

  enum class MutexMode : int {
    global,
    none,
    posix,
    abt
  };

  UnQLiteBackend() = default;

  UnQLiteBackend(UnQLiteBackend &&) = delete;

  UnQLiteBackend(const UnQLiteBackend &) = delete;

  UnQLiteBackend &operator=(UnQLiteBackend &&) = delete;

  UnQLiteBackend &operator=(const UnQLiteBackend &) = delete;

  static std::unique_ptr<Backend> create(const tl::engine &engine,
                                         const tl::pool &pool,
                                         const json &config);

  static std::unique_ptr<Backend> attach(const tl::engine &engine,
                                         const tl::pool &pool,
                                         const json &config);

  virtual ~UnQLiteBackend() {
    if (m_db)
      unqlite_commit(m_db);
    // if(m_db) unqlite_close(m_db); // XXX commented because of bug
  }

  virtual RequestResult<bool>
  createCollection(const std::string &coll_name) override {
    constexpr static const char *script = R"jx9(
        if(db_exists($collection)) {
            $ret = false;
            $err = "Collection already exists";
        } else {
            $ret = db_create($collection);
            if(!$ret) {
                $err = db_errlog();
            }
        }
        )jx9";
    RequestResult<bool> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, script, this);
      vm.set("collection", coll_name);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      }
      unqlite_commit(m_db);
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  virtual RequestResult<bool>
  openCollection(const std::string &coll_name) override {
    constexpr static const char *script = R"jx9(
        if(db_exists($collection)) {
            $ret = true;
        } else {
            $ret = false;
        }
        )jx9";
    RequestResult<bool> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, script, this);
      vm.set("collection", coll_name);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      result.error() = "Collection"s + coll_name + " does not exist";
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  virtual RequestResult<bool>
  dropCollection(const std::string &coll_name) override {
    constexpr static const char *script = R"jx9( 
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            $ret = db_drop_collection($collection);
            if(!$ret) {
                $err = db_errlog();
            }
        }
        )jx9";
    RequestResult<bool> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, script, this);
      vm.set("collection", coll_name);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      }
      unqlite_commit(m_db);
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  virtual RequestResult<uint64_t> store(const std::string &coll_name,
                                        const std::string &record,
                                        bool commit) override {
    if(m_bypass) {
        return storeDirect(coll_name, json::parse(record), commit);
    }
    std::ostringstream ss;
    ss << "$input = " << record << ";"
       << R"jx9(
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            $ret = db_store($collection,$input);
            if(!$ret) {
                $err = db_errlog();
            } else {
                $id = $input.__id;
            }
        }
        )jx9";
    RequestResult<uint64_t> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, ss.str().c_str(), this);
      vm.set("collection", coll_name);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      } else {
        result.value() = vm.get<uint64_t>("id");
      }
      if (commit)
        unqlite_commit(m_db);
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  virtual RequestResult<uint64_t> storeJson(const std::string &coll_name,
                                            const JsonWrapper &record,
                                            bool commit) override {
    if(m_bypass) {
        return storeDirect(coll_name, record, commit);
    }
    constexpr static const char *script = R"jx9(
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            $ret = db_store($collection,$input);
            if(!$ret) {
                $err = db_errlog();
            } else {
                $id = $input.__id;
            }
        }
        )jx9";
    RequestResult<uint64_t> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, script, this);
      vm.set("input", record.m_object);
      vm.set("collection", coll_name);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      } else {
        result.value() = vm.get<uint64_t>("id");
      }
      if (commit)
        unqlite_commit(m_db);
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  RequestResult<uint64_t> storeDirect(const std::string &coll_name,
                                      const JsonWrapper &record,
                                      bool commit) {
    RequestResult<uint64_t> result;
    if(!record->is_object()) {
        result.success() = false;
        result.error() = "Record should be an object";
        return result;
    }
    auto value = UnQLiteJsonEncoder::encode(record.m_object);
    std::vector<char> header(24);
    unqlite_int64 header_size = 24;
    std::unique_lock<tl::mutex> lock;
    if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
    // get the header (this is also a way to check that the collection exists)
    int rc = unqlite_kv_fetch(m_db, coll_name.c_str(), coll_name.size(),
                              header.data(), &header_size);
    if(rc == UNQLITE_NOTFOUND) {
        result.success() = false;
        result.error() = "Collection does not exist";
        return result;
    }
    // get the last_record_id and total_records from the header
    uint64_t last_record_id, total_records;
    std::memcpy(&last_record_id, header.data()+2, 8);
    std::memcpy(&total_records, header.data()+10, 8);
    // add the id to the record (it's kind of a hack)
    json id_rec = json::object();
    id_rec["__id"] = last_record_id;
    auto id_rec_buf = UnQLiteJsonEncoder::encode(id_rec);
    value.resize(value.size()+id_rec_buf.size()-2);
    std::memcpy(value.data()+value.size()-id_rec_buf.size()+1,
                id_rec_buf.data()+1, id_rec_buf.size()-1);
    std::string key = coll_name + "_";
    if(Endian::little) {
        last_record_id = Endian::swap(last_record_id);
        total_records = Endian::swap(total_records);
        key += std::to_string(last_record_id);
        result.value() = last_record_id;
        last_record_id += 1;
        total_records += 1;
        last_record_id = Endian::swap(last_record_id);
        total_records = Endian::swap(total_records);
    } else {
        key += std::to_string(last_record_id);
        result.value() = last_record_id;
        last_record_id += 1;
        total_records += 1;
    }
    // write the value
    rc = unqlite_kv_store(m_db, key.c_str(), key.size(),
                          value.data(), value.size());
    // update the header
    std::memcpy(header.data()+2, &last_record_id, 8);
    std::memcpy(header.data()+10, &last_record_id, 8);
    // write the header
    rc = unqlite_kv_store(m_db, coll_name.c_str(), coll_name.size(),
                          header.data(), header_size);
    return result;
  }

  virtual RequestResult<std::vector<uint64_t>>
  storeMulti(const std::string &coll_name,
             const std::vector<std::string> &records, bool commit) override {
    if(m_bypass) {
        auto json_records = json::array();
        for(auto& r : records)
            json_records.push_back(json::parse(r));
        return storeMultiDirect(coll_name, std::move(json_records), commit);
    }
    std::ostringstream ss;
    ss << "$input = [";
    for (unsigned i = 0; i < records.size(); i++) {
      ss << records[i];
      if (i != records.size() - 1)
        ss << ",";
    }
    ss << "];"
       << R"jx9(
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            $ids = [];
            $ret = db_store($collection,$input);
            if($ret) {
                foreach($input as $x) {
                    array_push($ids,$x.__id);
                }
            } else {
                $err = "Some records could not be stored";
            }
        }
        )jx9";
    RequestResult<std::vector<uint64_t>> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, ss.str().c_str(), this);
      vm.set("collection", coll_name);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      } else {
        result.value() = vm.get<std::vector<uint64_t>>("ids");
      }
      if (commit)
        unqlite_commit(m_db);
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  virtual RequestResult<bool> commit() override {
    RequestResult<bool> result;
    result.success() = true;
    std::unique_lock<tl::mutex> lock;
    if (m_mutex_mode == MutexMode::global)
      lock = std::unique_lock<tl::mutex>(m_mutex);
    unqlite_commit(m_db);
    return result;
  }

  virtual RequestResult<std::vector<uint64_t>>
  storeMultiJson(const std::string &coll_name, const JsonWrapper &records,
                 bool commit) override {
    if(m_bypass) {
        return storeMultiDirect(coll_name, records, commit);
    }
    RequestResult<std::vector<uint64_t>> result;
    if(!records->is_array()) {
        result.success() = false;
        result.error() = "storeMultiJson expecting Json array";
    }
    constexpr static const char *script = R"jx9(
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            $ids = [];
            $ret = db_store($collection,$input);
            if($ret) {
                foreach($input as $x) {
                    array_push($ids,$x.__id);
                }
            } else {
                $err = "Some records could not be stored";
            }
        }
        )jx9";
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, script, this);
      vm.set("input", records.m_object);
      vm.set("collection", coll_name);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      } else {
        result.value() = vm.get<std::vector<uint64_t>>("ids");
      }
      if (commit)
        unqlite_commit(m_db);
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  RequestResult<std::vector<uint64_t>>
  storeMultiDirect(const std::string &coll_name,
                   const JsonWrapper &records,
                   bool commit) {
    RequestResult<std::vector<uint64_t>> result;
    if(!records->is_array()) {
        result.success() = false;
        result.error() = "Records should be an array";
        return result;
    }
    std::vector<std::vector<char>> values;
    for(auto& obj : records.m_object) {
        if(!obj.is_object()) {
            result.success() = false;
            result.error() = "On of the records is not an object";
            return result;
        }
        values.push_back(UnQLiteJsonEncoder::encode(obj));
    }

    std::vector<char> header(24);
    unqlite_int64 header_size = 24;

    std::unique_lock<tl::mutex> lock;
    if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);

    // get the header (this is also a way to check that the collection exists)
    int rc = unqlite_kv_fetch(m_db, coll_name.c_str(), coll_name.size(),
                              header.data(), &header_size);
    if(rc == UNQLITE_NOTFOUND) {
        result.success() = false;
        result.error() = "Collection does not exist";
        return result;
    }

    // get the last_record_id and total_records from the header
    uint64_t last_record_id, total_records;
    std::memcpy(&last_record_id, header.data()+2, 8);
    std::memcpy(&total_records, header.data()+10, 8);

    auto record_id = last_record_id;
    if(Endian::little) record_id = Endian::swap(record_id);

    json id_rec = json::object();

    for(size_t i = 0; i < values.size(); i++) {
        auto& value = values[i];
        // add the id to the record (it's kind of a hack)
        id_rec["__id"] = Endian::big ? record_id : Endian::swap(record_id);
        auto id_rec_buf = UnQLiteJsonEncoder::encode(id_rec);
        value.resize(value.size()+id_rec_buf.size()-2);
        std::memcpy(value.data()+value.size()-id_rec_buf.size()+1,
                    id_rec_buf.data()+1, id_rec_buf.size()-1);
        std::string key = coll_name + "_"
            + std::to_string(Endian::big ? record_id : Endian::swap(record_id));

        // write the value
        rc = unqlite_kv_store(m_db, key.c_str(), key.size(),
                value.data(), value.size());
        result.value().push_back(record_id);
        record_id += 1;
    }

    if(Endian::little) {
        last_record_id = Endian::swap(last_record_id);
        total_records = Endian::swap(total_records);
        last_record_id += values.size();
        total_records += values.size();
        last_record_id = Endian::swap(last_record_id);
        total_records = Endian::swap(total_records);
    } else {
        last_record_id += values.size();
        total_records += values.size();
    }

    // update the header
    std::memcpy(header.data()+2, &last_record_id, 8);
    std::memcpy(header.data()+10, &last_record_id, 8);
    // write the header
    rc = unqlite_kv_store(m_db, coll_name.c_str(), coll_name.size(),
            header.data(), header_size);
    return result;
  }

  virtual RequestResult<std::string> fetch(const std::string &coll_name,
                                           uint64_t record_id) override {
    constexpr static const char *script = R"jx9(
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            $output = db_fetch_by_id($collection,$id);
            if($output == NULL) {
                $ret = false;
                $err = "Record does not exist";
            } else {
                $ret = true;
            }
        }
        )jx9";
    RequestResult<std::string> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, script, this);
      vm.set("collection", coll_name);
      vm.set("id", record_id);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      } else {
        std::ostringstream ss;
        vm["output"].printToStream(ss);
        result.value() = ss.str();
      }
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  virtual RequestResult<JsonWrapper> fetchJson(const std::string &coll_name,
                                               uint64_t record_id) override {
    constexpr static const char *script = R"jx9(
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            $output = db_fetch_by_id($collection,$id);
            if($output == NULL) {
                $ret = false;
                $err = "Record does not exist";
            } else {
                $ret = true;
            }
        }
        )jx9";
    RequestResult<JsonWrapper> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, script, this);
      vm.set("collection", coll_name);
      vm.set("id", record_id);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      } else {
        result.value() = vm["output"].as<json>();
      }
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  virtual RequestResult<std::vector<std::string>>
  fetchMulti(const std::string &coll_name,
             const std::vector<uint64_t> &record_ids) override {
    constexpr static const char *script = R"jx9(
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            $output = [];
            $ret = true;
            foreach($ids as $id) {
                $x = db_fetch_by_id($collection,$id);
                array_push($output, $x);
            }
        }
        )jx9";
    RequestResult<std::vector<std::string>> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, script, this);
      vm.set("collection", coll_name);
      vm.set("ids", record_ids);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      } else {
        UnQLiteValue output = vm["output"];
        output.foreach ([&result](unsigned, const UnQLiteValue &v) {
          std::ostringstream ss;
          v.printToStream(ss);
          result.value().push_back(std::move(ss.str()));
        });
      }
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  virtual RequestResult<JsonWrapper>
  fetchMultiJson(const std::string &coll_name,
                 const std::vector<uint64_t> &record_ids) override {
    constexpr static const char *script = R"jx9(
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            $output = [];
            $ret = true;
            foreach($ids as $id) {
                $x = db_fetch_by_id($collection,$id);
                array_push($output, $x);
            }
        }
        )jx9";
    RequestResult<JsonWrapper> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, script, this);
      vm.set("collection", coll_name);
      vm.set("ids", record_ids);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      } else {
        result.value() = vm["output"].as<json>();
      }
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  virtual RequestResult<std::vector<std::string>>
  filter(const std::string &coll_name,
         const std::string &filter_code) override {
    const std::string script = "$filter_cb = "s + filter_code + ";" +
                               R"jx9(
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            if(0 == db_total_records($collection)) {
                $ret = TRUE;
                $data = [];
            } else {
                $ret = db_fetch_all($collection, $filter_cb);
                if($ret == FALSE) {
                    $ret = TRUE;
                    $data = [];
                } else {
                    $data = $ret;
                    $ret = TRUE;
                }
            }
        }
        )jx9";
    RequestResult<std::vector<std::string>> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, script.c_str(), this);
      vm.registerSonataFunctions();
      vm.set("collection", coll_name);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      } else {
        std::vector<std::string> array;
        UnQLiteValue uql_values = vm["data"];
        uql_values.foreach ([&array](unsigned index, const UnQLiteValue &val) {
          std::ostringstream ss;
          val.printToStream(ss);
          array.push_back(ss.str());
        });
        result.value() = std::move(array);
      }
      unqlite_commit(m_db);
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  virtual RequestResult<JsonWrapper>
  filterJson(const std::string &coll_name,
             const std::string &filter_code) override {
    const std::string script = "$filter_cb = "s + filter_code +
                               ";"
                               R"jx9(
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            if(0 == db_total_records($collection)) {
                $ret = TRUE;
                $data = [];
            } else {
                $ret = db_fetch_all($collection, $filter_cb);
                if($ret == FALSE) {
                    $ret = TRUE;
                    $data = [];
                } else {
                    $data = $ret;
                    $ret = TRUE;
                }
            }
        }
        )jx9";
    RequestResult<JsonWrapper> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, script.c_str(), this);
      vm.registerSonataFunctions();
      vm.set("collection", coll_name);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      } else {
        result.value() = vm["data"].as<json>();
      }
      unqlite_commit(m_db);
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  virtual RequestResult<bool> update(const std::string &coll_name,
                                     uint64_t record_id,
                                     const std::string &new_content,
                                     bool commit) override {
    const std::string script = "$input = "s + new_content + ";" + R"jx9(
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            $ret = db_update_record($collection,$record_id,$input);
            if(!$ret) {
                $err = db_errlog();
            }
        }
        )jx9";
    RequestResult<bool> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, script.c_str(), this);
      vm.set("collection", coll_name);
      vm.set("record_id", record_id);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      }
      if (commit)
        unqlite_commit(m_db);
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  virtual RequestResult<bool> updateJson(const std::string &coll_name,
                                         uint64_t record_id,
                                         const JsonWrapper &new_content,
                                         bool commit) override {
    constexpr static const char *script = R"jx9(
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            $ret = db_update_record($collection,$record_id,$input);
            if(!$ret) {
                $err = db_errlog();
            }
        }
        )jx9";
    RequestResult<bool> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, script, this);
      vm.set("input", new_content.m_object);
      vm.set("collection", coll_name);
      vm.set("record_id", record_id);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      }
      if (commit)
        unqlite_commit(m_db);
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  virtual RequestResult<std::vector<bool>> updateMulti(
      const std::string &coll_name, const std::vector<uint64_t> &record_ids,
      const std::vector<std::string> &new_contents, bool commit) override {
    std::ostringstream ss;
    ss << "$input = [";
    for (unsigned i = 0; i < new_contents.size(); i++) {
      ss << new_contents[i];
      if (i != new_contents.size() - 1)
        ss << ",";
    }
    ss << "];"
       << R"jx9(
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            //while(!db_begin()) {}
            $ret = true;
            $result = [];
            for($i = 0; $i < count($record_ids); $i++) {
                $ret = db_update_record($collection, $record_ids[$i], $input[$i]);
                array_push($result, $ret);
              //  if(!$ret) {
              //      $err = db_errlog();
              //      db_rollback();
              //  }
            }
            $ret = true;
            //if($ret) { db_commit(); }
        }
        )jx9";
    RequestResult<std::vector<bool>> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, ss.str().c_str(), this);
      vm.set("collection", coll_name);
      vm.set("record_ids", record_ids);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      } else {
        result.value() = vm.get<std::vector<bool>>("result");
      }
      if (commit)
        unqlite_commit(m_db);
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  virtual RequestResult<std::vector<bool>>
  updateMultiJson(const std::string &coll_name,
                  const std::vector<uint64_t> &record_ids,
                  const JsonWrapper &new_contents, bool commit) override {
    constexpr static const char *script = R"jx9(
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            //while(!db_begin()) {}
            $result = [];
            $ret = true;
            for($i = 0; $i < count($record_ids); $i++) {
                $ret = db_update_record($collection, $record_ids[$i], $input[$i]);
              //  if(!$ret) {
              //      $err = db_errlog();
              //      db_rollback();
              //  }
                array_push($result, $ret);
            }
            $ret = true;
            //if($ret) { db_commit(); }
        }
        )jx9";
    RequestResult<std::vector<bool>> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, script, this);
      vm.set("input", new_contents.m_object);
      vm.set("collection", coll_name);
      vm.set("record_ids", record_ids);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      } else {
        result.value() = vm.get<std::vector<bool>>("result");
      }
      if (commit)
        unqlite_commit(m_db);
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  virtual RequestResult<std::vector<std::string>>
  all(const std::string &coll_name) override {
    constexpr static const char *script = R"jx9(
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            if(0 == db_total_records($collection)) {
                $ret = TRUE;
                $data = [];
            } else {
                $ret = db_fetch_all($collection);
                if($ret == FALSE) {
                    $err = db_errlog();
                } else {
                    $data = $ret;
                    $ret = TRUE;
                }
            }
        }
        )jx9";
    RequestResult<std::vector<std::string>> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, script, this);
      vm.set("collection", coll_name);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      } else {
        std::vector<std::string> array;
        UnQLiteValue uql_values = vm["data"];
        uql_values.foreach ([&array](unsigned index, const UnQLiteValue &val) {
          std::ostringstream ss;
          val.printToStream(ss);
          array.push_back(ss.str());
        });
        result.value() = std::move(array);
      }
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  virtual RequestResult<JsonWrapper>
  allJson(const std::string &coll_name) override {
    constexpr static const char *script = R"jx9(
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            if(0 == db_total_records($collection)) {
                $ret = TRUE;
                $data = [];
            } else {
                $ret = db_fetch_all($collection);
                if($ret == FALSE) {
                    $err = db_errlog();
                } else {
                    $data = $ret;
                    $ret = TRUE;
                }
            }
        }
        )jx9";
    RequestResult<JsonWrapper> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, script, this);
      vm.set("collection", coll_name);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      } else {
        result.value() = vm["data"].as<json>();
      }
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  virtual RequestResult<uint64_t>
  lastID(const std::string &coll_name) override {
    constexpr static const char *script = R"jx9(
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            $id = db_last_record_id($collection);
            if($id == FALSE) {
                $ret = false;
                $err = db_errlog();
            } else {
                $ret = true;
            }
        }
        )jx9";
    RequestResult<uint64_t> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, script, this);
      vm.set("collection", coll_name);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      } else {
        result.value() = vm["id"];
      }
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  virtual RequestResult<size_t> size(const std::string &coll_name) override {
    constexpr static const char *script = R"jx9(
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            $size = db_total_records($collection);
            if($size == FALSE) {
                $ret = false;
                $err = db_errlog();
            } else {
                $ret = true;
            }
        }
        )jx9";
    RequestResult<size_t> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, script, this);
      vm.set("collection", coll_name);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      } else {
        result.value() = vm["size"];
      }
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  virtual RequestResult<bool> erase(const std::string &coll_name,
                                    uint64_t record_id, bool commit) override {
    constexpr static const char *script = R"jx9(
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            $rc = db_drop_record($collection, $id);
            if($rc) {
                $ret = true;
            } else {
                $ret = false;
                $err = "Failed to erase record";
            }
        }
        )jx9";
    RequestResult<bool> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, script, this);
      vm.set("collection", coll_name);
      vm.set("id", record_id);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      }
      if (commit)
        unqlite_commit(m_db);
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  virtual RequestResult<bool>
  eraseMulti(const std::string &coll_name,
             const std::vector<uint64_t> &record_ids, bool commit) override {
    constexpr static const char *script = R"jx9(
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            $ret = true;
            foreach($ids as $id) {
                $rc = db_drop_record($collection, $id);
                if($rc) {
                    $ret = true;
                } else {
                    $ret = false;
                    $err = "Failed to erase record";
                    break;
                }
            }
        }
        )jx9";
    RequestResult<bool> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, script, this);
      vm.set("collection", coll_name);
      vm.set("ids", record_ids);
      vm.execute();
      result.success() = vm.get<bool>("ret");
      if (!result.success()) {
        result.error() = vm.get<std::string>("err");
      }
      if (commit)
        unqlite_commit(m_db);
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
    }
    return result;
  }

  virtual RequestResult<std::unordered_map<std::string, std::string>>
  execute(const std::string &code, const std::unordered_set<std::string> &vars,
          bool commit) override {
    RequestResult<std::unordered_map<std::string, std::string>> result;
    try {
      std::unique_lock<tl::mutex> lock;
      if (m_mutex_mode == MutexMode::global)
        lock = std::unique_lock<tl::mutex>(m_mutex);
      UnQLiteVM vm(m_db, code.c_str(), this);
      vm.registerSonataFunctions();
      vm.execute();
      result.success() = true;
      for (auto &name : vars) {
        if (name != "__output__") {
          auto val = vm[name];
          std::ostringstream ss;
          val.printToStream(ss);
          result.value().emplace(name, ss.str());
        } else {
          result.value().emplace("__output__", vm.output());
        }
      }
      if (commit)
        unqlite_commit(m_db);
    } catch (const Exception &e) {
      result.success() = false;
      result.error() = e.what();
      result.value().clear();
    }
    return result;
  }

  virtual RequestResult<bool> destroy() override {
    RequestResult<bool> result;
    if (m_db)
      unqlite_close(m_db);
    m_db = nullptr;
    if (remove(m_filename.c_str()) != 0) {
      result.success() = false;
      result.error() = "Could not remove file: "s + strerror(errno);
    }
    return result;
  }

  std::string getConfig() const override {
    return "{\"path\": \""s + m_filename + "\"" +
           ", \"temporary\": " + (m_is_temporary ? "true" : "false") +
           ", \"in-memory\": " + (m_is_in_memory ? "true" : "false") + "}";
  }

private:
  unqlite *m_db = nullptr;
  std::string m_filename;
  bool m_is_temporary;
  bool m_is_in_memory;
  bool m_bypass;
  MutexMode m_mutex_mode = MutexMode::global;
  tl::mutex m_mutex; // used only if mutex_mode is "global"

  Client m_client;
  Admin m_admin;
};

} // namespace sonata
#endif
