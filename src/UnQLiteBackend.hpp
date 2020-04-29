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

    static std::unique_ptr<Backend> create(thallium::engine& engine, const Json::Value& config);
    
    static std::unique_ptr<Backend> attach(thallium::engine& engine, const Json::Value& config);

    virtual ~UnQLiteBackend() {
        if(m_db) unqlite_close(m_db);
    }

    virtual RequestResult<bool> createCollection(
            const std::string& coll_name) override {
        constexpr static const char* script = 
        "if(db_exists($collection)) {"
            "$ret = false;"
            "$err = \"Collection already exists\";"
        "} else {"
            "$ret = db_create($collection);"
            "if(!$ret) {"
                "$err = db_errlog();"
            "}"
        "}";
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
        constexpr static const char* script = 
        "if(db_exists($collection)) {"
            "$ret = true;"
        "} else {"
            "$ret = false;"
        "}";
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
        constexpr static const char* script = 
        "if(!db_exists($collection)) {"
            "$ret = false;"
            "$err = \"Collection does not exist\";"
        "} else {"
            "$ret = db_drop_collection($collection);"
            "if(!$ret) {"
                "$err = db_errlog();"
            "}"
        "}";
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
            const std::string& record) override {
        std::ostringstream ss;
        ss << "$input = " << record << ";"
        <<
        "if(!db_exists($collection)) {"
            "$ret = false;"
            "$err = \"Collection does not exist\";"
        "} else {"
            "$ret = db_store($collection,$input);"
            "if(!$ret) {"
                "$err = db_errlog();"
            "} else {"
                "$id = $input.__id;"
            "}"
        "}";
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
            unqlite_commit(m_db);
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<std::string> fetch(
            const std::string& coll_name,
            uint64_t record_id) override {
        constexpr static const char* script =
        "if(!db_exists($collection)) {"
            "$ret = false;"
            "$err = \"Collection does not exist\";"
        "} else {"
            "$output = db_fetch_by_id($collection,$id);"
            "if($output == NULL) {"
                "$ret = false;"
                "$err = \"Record does not exist\";"
            "} else {"
                "$ret = true;"
            "}"
        "}";
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

    virtual RequestResult<std::vector<std::string>> filter(
            const std::string& coll_name,
            const std::string& filter_code) override {
        std::string script = "$filter_cb = "s
            + filter_code + ";"
        "if(!db_exists($collection)) {"
            "$ret = false;"
            "$err = \"Collection does not exist\";"
        "} else {"
            "if(0 == db_total_records($collection)) {"
                "$ret = TRUE;"
                "$data = [];"
            "} else {"
                "$ret = db_fetch_all($collection, $filter_cb);"
                "if($ret == FALSE) {"
                    "$ret = TRUE;"
                    "$data = [];"
                "} else {"
                    "$data = $ret;"
                    "$ret = TRUE;"
                "}"
            "}"
        "}";
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

    virtual RequestResult<bool> update(
            const std::string& coll_name,
            uint64_t record_id,
            const std::string& new_content) override {
        std::ostringstream ss;
        ss << "$input = " << new_content << ";"
        <<
        "if(!db_exists($collection)) {"
            "$ret = false;"
            "$err = \"Collection does not exist\";"
        "} else {"
            "$ret = db_update_record($collection,$record_id,$input);"
            "if(!$ret) {"
                "$err = db_errlog();"
            "}"
        "}";
        RequestResult<bool> result;
        try {
            std::unique_lock<tl::mutex> lock;
            if(!m_unqlite_is_threadsafe)
                lock = std::unique_lock<tl::mutex>(m_mutex);
            UnQLiteVM vm(m_db, ss.str().c_str(), this);
            vm.set("collection", coll_name);
            vm.set("record_id", record_id);
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

    virtual RequestResult<std::vector<std::string>> all(
            const std::string& coll_name) override {
        constexpr static const char* script =
        "if(!db_exists($collection)) {"
            "$ret = false;"
            "$err = \"Collection does not exist\";"
        "} else {"
            "if(0 == db_total_records($collection)) {"
                "$ret = TRUE;"
                "$data = [];"
            "} else {"
                "$ret = db_fetch_all($collection);"
                "if($ret == FALSE) {"
                    "$err = db_errlog();"
                "} else {"
                    "$data = $ret;"
                    "$ret = TRUE;"
                "}"
            "}"
        "}";
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

    virtual RequestResult<uint64_t> lastID(
            const std::string& coll_name) override {
        constexpr static const char* script =
        "if(!db_exists($collection)) {"
            "$ret = false;"
            "$err = \"Collection does not exist\";"
        "} else {"
            "$id = db_last_record_id($collection);"
            "if($id == FALSE) {"
                "$ret = false;"
                "$err = db_errlog();"
            "} else {"
                "$ret = true;"
            "}"
        "}";
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
        constexpr static const char* script =
        "if(!db_exists($collection)) {"
            "$ret = false;"
            "$err = \"Collection does not exist\";"
        "} else {"
            "$size = db_total_records($collection);"
            "if($size == FALSE) {"
                "$ret = false;"
                "$err = db_errlog();"
            "} else {"
                "$ret = true;"
            "}"
        "}";
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
            uint64_t record_id) override {
        constexpr static const char* script =
        "if(!db_exists($collection)) {"
            "$ret = false;"
            "$err = \"Collection does not exist\";"
        "} else {"
            "$rc = db_drop_record($collection, $id);"
            "if($rc) {"
                "$ret = true;"
            "} else {"
                "$ret = false;"
                "$err = \"Failed to erase record \";"
            "}"
        "}";
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
            unqlite_commit(m_db);
        } catch(const Exception& e) {
            result.success() = false;
            result.error() = e.what();
        }
        return result;
    }

    virtual RequestResult<std::unordered_map<std::string,std::string>>
        execute(const std::string& code,
                const std::unordered_set<std::string>& vars) override {
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
            unqlite_commit(m_db);
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
