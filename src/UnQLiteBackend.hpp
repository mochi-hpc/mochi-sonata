/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_UNQLITE_BACKEND_HPP
#define __SONATA_UNQLITE_BACKEND_HPP
#include "sonata/Client.hpp"
#include "sonata/Admin.hpp"
#include "sonata/Backend.hpp"
#include "unqlite/jx9.h"
#include "unqlite/unqlite.h"

#include "UnQLiteVM.hpp"
#include "UnQLiteMutex.hpp"

#include <spdlog/spdlog.h>
#include <json/json.h>
#include <cstdio>
#include <sstream>
#include <fstream>
#include <thallium.hpp>

namespace sonata {

namespace tl = thallium;
using namespace std::string_literals;

class UnQLiteVM;

class UnQLiteBackend : public Backend {

    friend class UnQLiteVM;

    public:

    UnQLiteBackend() {
        m_unqlite_is_threadsafe = unqlite_lib_is_threadsafe();
    }

    UnQLiteBackend(UnQLiteBackend&&) = delete;

    UnQLiteBackend(const UnQLiteBackend&) = delete;

    UnQLiteBackend& operator=(UnQLiteBackend&&) = delete;

    UnQLiteBackend& operator=(const UnQLiteBackend&) = delete;

    static std::unique_ptr<Backend> create(const thallium::engine& engine, const Json::Value& config);
    
    static std::unique_ptr<Backend> attach(const thallium::engine& engine, const Json::Value& config);

    virtual ~UnQLiteBackend() {
        //if(m_db) unqlite_close(m_db); // XXX commented because of bug
    }

    virtual RequestResult<bool> createCollection(
            const std::string& coll_name) override {
        constexpr static const char* script = R"jx9(
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
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, script, this);
            vm.set("collection", coll_name);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            }
            unqlite_commit(m_db);
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<bool> openCollection(
            const std::string& coll_name) override {
        constexpr static const char* script = R"jx9(
        if(db_exists($collection)) {
            $ret = true;
        } else {
            $ret = false;
        }
        )jx9";
        RequestResult<bool> result;
        try {
            std::unique_lock<tl::mutex> lock;
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, script, this);
            vm.set("collection", coll_name);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            result.error() = "Collection"s + coll_name + " does not exist";
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<bool> dropCollection(
            const std::string& coll_name) override {
        constexpr static const char* script = R"jx9( 
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
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, script, this);
            vm.set("collection", coll_name);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            }
            unqlite_commit(m_db);
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<uint64_t> store(
            const std::string& coll_name,
            const std::string& record,
            bool commit) override {
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
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, ss.str().c_str(), this);
            vm.set("collection", coll_name);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            } else {
                result.value() = vm.get<uint64_t>("id");
            }
            if(commit) unqlite_commit(m_db);
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<uint64_t> storeJson(
            const std::string& coll_name,
            const Json::Value& record,
            bool commit) override {
        constexpr static const char* script = R"jx9(
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
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, script, this);
            vm.set("input", record);
            vm.set("collection", coll_name);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            } else {
                result.value() = vm.get<uint64_t>("id");
            }
            if(commit) unqlite_commit(m_db);
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }
    
    virtual RequestResult<std::vector<uint64_t>> storeMulti(
            const std::string& coll_name,
            const std::vector<std::string>& records,
            bool commit) override {
        std::ostringstream ss;
        ss << "$input = [";
        for(unsigned i=0; i < records.size(); i++) {
            ss << records[i];
            if(i != records.size()-1) ss << ",";
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
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, ss.str().c_str(), this);
            vm.set("collection", coll_name);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            } else {
                result.value() = vm.get<std::vector<uint64_t>>("ids");
            }
            if(commit) unqlite_commit(m_db);
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }
    
    virtual RequestResult<bool> commit() override {
        RequestResult<bool> result;
        result.success() = true;
        std::unique_lock<tl::mutex> lock;
        if(!m_unqlite_is_threadsafe)
            lock = std::unique_lock<tl::mutex>(m_mutex);
        unqlite_commit(m_db);
        return result;
    }

    virtual RequestResult<std::vector<uint64_t>> storeMultiJson(
            const std::string& coll_name,
            const Json::Value& records,
            bool commit) override {
        constexpr static const char* script = R"jx9(
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
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, script, this);
            vm.set("input", records);
            vm.set("collection", coll_name);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            } else {
                result.value() = vm.get<std::vector<uint64_t>>("ids");
            }
            if(commit) unqlite_commit(m_db);
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<std::string> fetch(
            const std::string& coll_name,
            uint64_t record_id) override {
        constexpr static const char* script = R"jx9(
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
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, script, this);
            vm.set("collection", coll_name);
            vm.set("id", record_id);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            } else {
                std::ostringstream ss;
                vm["output"].printToStream(ss);
                result.value() = ss.str();
            }
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<Json::Value> fetchJson(
            const std::string& coll_name,
            uint64_t record_id) override {
        constexpr static const char* script = R"jx9(
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
        RequestResult<Json::Value> result;
        try {
            std::unique_lock<tl::mutex> lock;
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, script, this);
            vm.set("collection", coll_name);
            vm.set("id", record_id);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            } else {
                result.value() = vm["output"];
            }
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<std::vector<std::string>> fetchMulti(
            const std::string& coll_name,
            const std::vector<uint64_t>& record_ids) override {
        constexpr static const char* script = R"jx9(
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
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, script, this);
            vm.set("collection", coll_name);
            vm.set("ids", record_ids);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            } else {
                UnQLiteValue output = vm["output"];
                output.foreach([&result](unsigned, const UnQLiteValue& v) {
                    std::ostringstream ss;
                    v.printToStream(ss);
                    result.value().push_back(std::move(ss.str()));
                });
            }
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<Json::Value> fetchMultiJson(
            const std::string& coll_name,
            const std::vector<uint64_t>& record_ids) override {
        constexpr static const char* script = R"jx9(
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
        RequestResult<Json::Value> result;
        try {
            std::unique_lock<tl::mutex> lock;
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, script, this);
            vm.set("collection", coll_name);
            vm.set("ids", record_ids);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            } else {
                result.value() = vm["output"];
            }
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<std::vector<std::string>> filter(
            const std::string& coll_name,
            const std::string& filter_code) override {
        const std::string script =
        "$filter_cb = "s
            + filter_code + ";" +
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
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, script.c_str(), this);
            vm.registerSonataFunctions();
            vm.set("collection", coll_name);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            } else {
                std::vector<std::string> array;
                UnQLiteValue uql_values = vm["data"];
                uql_values.foreach([&array](unsigned index, const UnQLiteValue& val) {
                        std::ostringstream ss;
                        val.printToStream(ss);
                        array.push_back(ss.str());
                    });
                result.value() = std::move(array);
            }
            unqlite_commit(m_db);
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<Json::Value> filterJson(
            const std::string& coll_name,
            const std::string& filter_code) override {
        const std::string script =
        "$filter_cb = "s
            + filter_code + ";"
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
        RequestResult<Json::Value> result;
        try {
            std::unique_lock<tl::mutex> lock;
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, script.c_str(), this);
            vm.registerSonataFunctions();
            vm.set("collection", coll_name);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            } else {
                result.value() = vm["data"];
            }
            unqlite_commit(m_db);
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<bool> update(
            const std::string& coll_name,
            uint64_t record_id,
            const std::string& new_content,
            bool commit) override {
        const std::string script =
        "$input = "s + new_content + ";"
        + R"jx9(
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
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, script.c_str(), this);
            vm.set("collection", coll_name);
            vm.set("record_id", record_id);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            }
            if(commit) unqlite_commit(m_db);
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<bool> updateJson(
            const std::string& coll_name,
            uint64_t record_id,
            const Json::Value& new_content,
            bool commit) override {
        constexpr static const char* script = R"jx9(
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
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, script, this);
            vm.set("input", new_content);
            vm.set("collection", coll_name);
            vm.set("record_id", record_id);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            }
            if(commit) unqlite_commit(m_db);
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<std::vector<bool>> updateMulti(
            const std::string& coll_name,
            const std::vector<uint64_t>& record_ids,
            const std::vector<std::string>& new_contents,
            bool commit) override {
        std::ostringstream ss;
        ss << "$input = [";
        for(unsigned i=0; i < new_contents.size(); i++) {
            ss << new_contents[i];
            if(i != new_contents.size()-1) ss << ",";
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
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, ss.str().c_str(), this);
            vm.set("collection", coll_name);
            vm.set("record_ids", record_ids);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            } else {
                result.value() = vm.get<std::vector<bool>>("result");
            }
            if(commit) unqlite_commit(m_db);
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<std::vector<bool>> updateMultiJson(
            const std::string& coll_name,
            const std::vector<uint64_t>& record_ids,
            const Json::Value& new_contents,
            bool commit) override {
        constexpr static const char* script = R"jx9(
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
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, script, this);
            vm.set("input", new_contents);
            vm.set("collection", coll_name);
            vm.set("record_ids", record_ids);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            } else {
                result.value() = vm.get<std::vector<bool>>("result");
            }
            if(commit) unqlite_commit(m_db);
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<std::vector<std::string>> all(
            const std::string& coll_name) override {
        constexpr static const char* script = R"jx9(
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
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, script, this);
            vm.set("collection", coll_name);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            } else {
                std::vector<std::string> array;
                UnQLiteValue uql_values = vm["data"];
                uql_values.foreach([&array](unsigned index, const UnQLiteValue& val) {
                        std::ostringstream ss;
                        val.printToStream(ss);
                        array.push_back(ss.str());
                    });
                result.value() = std::move(array);
            }
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<Json::Value> allJson(
            const std::string& coll_name) override {
        constexpr static const char* script = R"jx9(
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
        RequestResult<Json::Value> result;
        try {
            std::unique_lock<tl::mutex> lock;
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, script, this);
            vm.set("collection", coll_name);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            } else {
                result.value() = vm["data"];
            }
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<uint64_t> lastID(
            const std::string& coll_name) override {
        constexpr static const char* script = R"jx9(
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
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, script, this);
            vm.set("collection", coll_name);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            } else {
                result.value() = vm["id"];
            }
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<size_t> size(
            const std::string& coll_name) override {
        constexpr static const char* script = R"jx9(
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
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, script, this);
            vm.set("collection", coll_name);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            } else {
                result.value() = vm["size"];
            }
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<bool> erase(
            const std::string& coll_name,
            uint64_t record_id,
            bool commit) override {
        constexpr static const char* script = R"jx9(
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
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, script, this);
            vm.set("collection", coll_name);
            vm.set("id", record_id);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            }
            if(commit) unqlite_commit(m_db);
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<bool> eraseMulti(
            const std::string& coll_name,
            const std::vector<uint64_t>& record_ids,
            bool commit) override {
        constexpr static const char* script = R"jx9(
        if(!db_exists($collection)) {
            $ret = false;
            $err = "Collection does not exist";
        } else {
            $ret = true;
            //while(!db_begin()) {}
            foreach($ids as $id) {
                $rc = db_drop_record($collection, $id);
                if($rc) {
                    $ret = true;
                } else {
                    //db_rollback();
                    $ret = false;
                    //$err = "Failed to erase record";
                    //break;
                }
            }
            //if($ret) { db_commit(); }
        }
        )jx9";
        RequestResult<bool> result;
        try {
            std::unique_lock<tl::mutex> lock;
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, script, this);
            vm.set("collection", coll_name);
            vm.set("ids", record_ids);
            vm.execute();
            result.success() = vm.get<bool>("ret");
            if(!result.success()) {
                result.error() = vm.get<std::string>("err");
            }
            if(commit) unqlite_commit(m_db);
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<std::unordered_map<std::string,std::string>>
        execute(const std::string& code,
                const std::unordered_set<std::string>& vars,
                bool commit) override {
        RequestResult<std::unordered_map<std::string,std::string>> result;
        try {
            std::unique_lock<tl::mutex> lock;
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, code.c_str(), this);
            vm.registerSonataFunctions();
            vm.execute();
            result.success() = true;
            for(auto& name : vars) {
                if(name != "__output__") {
                    auto val = vm[name];
                    std::ostringstream ss;
                    val.printToStream(ss);
                    result.value().emplace(name, ss.str());
                } else {
                    result.value().emplace("__output__", vm.output());
                }
            }
            if(commit) unqlite_commit(m_db);
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
            result.value().clear();
        }
        return result;
    }

    virtual RequestResult<bool> destroy() override {
        RequestResult<bool> result;
        if(m_db) unqlite_close(m_db);
        m_db = nullptr;
        if(remove(m_filename.c_str()) != 0) {
            result.success() = false;
            result.error() = "Could not remove file: "s + strerror(errno);
        }
        return result;
    }

    private:

    unqlite*    m_db = nullptr;
    std::string m_filename;
    tl::mutex   m_mutex; // used only if unqlite doesn't have threads enables
    bool        m_unqlite_is_threadsafe;
    Client      m_client;
    Admin       m_admin;
};

}
#endif
