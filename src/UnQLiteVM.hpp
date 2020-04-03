#ifndef __SONATA_UNQLITE_VM_HPP
#define __SONATA_UNQLITE_VM_HPP

#include "UnQLiteValue.hpp"

#include <type_traits>
#include <string>
#include <spdlog/spdlog.h>

namespace sonata {

using namespace std::string_literals;

class UnQLiteVM {

    public:

    UnQLiteVM(unqlite* database, const char* code)
    : m_code(code)
    , m_db(database) {
        compile();
        registerSonataFunctions();
    }

    UnQLiteVM(UnQLiteVM&& other) = delete;
    UnQLiteVM(const UnQLiteVM& other) = delete;
    UnQLiteVM& operator=(const UnQLiteVM&) = delete;
    UnQLiteVM& operator=(UnQLiteVM&&) = delete;

    ~UnQLiteVM() {
        unqlite_vm_release(m_vm);
    }

    void execute() {
        int ret = unqlite_vm_exec(m_vm);
        if(ret != UNQLITE_OK) parse_and_throw_error();
    }

    void execute(const std::function<int(const void*, unsigned int)>& callback) {
        int ret = unqlite_vm_config(m_vm, UNQLITE_VM_CONFIG_OUTPUT,
                                    output_callback, const_cast<void*>(reinterpret_cast<const void*>(&callback)));
        if(ret != UNQLITE_OK) parse_and_throw_error();
        execute();
    }

    template<typename T>
    void set(const std::string& name, const T& value) {
        UnQLiteValue uvalue(value, m_vm);
        m_encoded_names.emplace_back(std::make_unique<std::string>(name));
        int ret = unqlite_vm_config(m_vm, UNQLITE_VM_CONFIG_CREATE_VAR,
                                    m_encoded_names.back()->c_str(), uvalue.m_value);
        if(ret != UNQLITE_OK) parse_and_throw_error();
    }

    template<typename T>
    T get(const std::string& name) const {
        unqlite_value* value = unqlite_vm_extract_variable(m_vm, name.c_str());
        return UnQLiteValue(value, m_vm, nullptr).as<T>();
    }

    std::string output() const {
        const char *pOut;
        unsigned int nLen;
        unqlite_vm_config(m_vm,
                 UNQLITE_VM_CONFIG_EXTRACT_OUTPUT,
                 &pOut, &nLen);
        return std::string(pOut, nLen);
    }

    UnQLiteValue operator[](const std::string& name) const {
        unqlite_value* value = unqlite_vm_extract_variable(m_vm, name.c_str());
        return UnQLiteValue(value, m_vm, nullptr);
    }

    private:

    const char* m_code = nullptr;
    unqlite*    m_db = nullptr;
    unqlite_vm* m_vm = nullptr;
    std::vector<std::unique_ptr<std::string>> m_encoded_names;

    void compile() {
        int ret = unqlite_compile(m_db, m_code, strlen(m_code), &m_vm);
        if(ret != UNQLITE_OK) parse_and_throw_error();
    }

    void registerSonataFunctions() {
        int ret;
        // Admin functions
        ret = unqlite_create_function(m_vm, "snta_db_create",      snta_db_create,      nullptr);
        ret = unqlite_create_function(m_vm, "snta_db_attach",      snta_db_attach,      nullptr);
        ret = unqlite_create_function(m_vm, "snta_db_detach",      snta_db_detach,      nullptr);
        ret = unqlite_create_function(m_vm, "snta_db_destroy",     snta_db_destroy,     nullptr);
        // Database functions
        ret = unqlite_create_function(m_vm, "sntd_coll_create",    sntd_coll_create,    nullptr);
        ret = unqlite_create_function(m_vm, "sntd_coll_exists",    sntd_coll_exists,    nullptr);
        ret = unqlite_create_function(m_vm, "sntd_coll_open",      sntd_coll_open,      nullptr);
        ret = unqlite_create_function(m_vm, "sntd_coll_drop",      sntd_coll_drop,      nullptr);
        ret = unqlite_create_function(m_vm, "sntd_execute",        sntd_execute,        nullptr);
        // Collection functions
        ret = unqlite_create_function(m_vm, "sntc_store",          sntc_store,          nullptr);
        ret = unqlite_create_function(m_vm, "sntc_fetch",          sntc_fetch,          nullptr);
        ret = unqlite_create_function(m_vm, "sntc_filter",         sntc_filter,         nullptr);
        ret = unqlite_create_function(m_vm, "sntc_update",         sntc_update,         nullptr);
        ret = unqlite_create_function(m_vm, "sntc_all",            sntc_all,            nullptr);
        ret = unqlite_create_function(m_vm, "sntc_last_record_id", sntc_last_record_id, nullptr);
        ret = unqlite_create_function(m_vm, "sntc_size",           sntc_size,           nullptr);
        ret = unqlite_create_function(m_vm, "sntc_erase",          sntc_erase,          nullptr);
        // Wait functions
        ret = unqlite_create_function(m_vm, "sntr_wait",           sntr_wait,           nullptr);
        ret = unqlite_create_function(m_vm, "sntr_test",           sntr_test,           nullptr);
    }

    void parse_and_throw_error() {
        const char *errorBuffer = nullptr;
        int len = 0;
        unqlite_config(m_db, UNQLITE_CONFIG_JX9_ERR_LOG, &errorBuffer, &len);
        std::string error = "UnQLite error: "s;
        if(len > 0) {
            error += std::string(errorBuffer, len);
            throw Exception(error);
        }
        unqlite_config(m_db, UNQLITE_CONFIG_ERR_LOG, &errorBuffer, &len);
        if(len > 0) {
            error += std::string(errorBuffer, len);
            throw Exception(error);
        }
        error += "(unknown)";
        throw Exception(error);
    }

    static int output_callback(const void *pOutput, unsigned int nOutputLen, void *pUserData) {
        auto function_ptr = static_cast<std::function<int(const void* , unsigned in)>*>(pUserData);
        if(function_ptr) {
            return (*function_ptr)(pOutput, nOutputLen);
        } else {
            return UNQLITE_OK;
        }
    }

    // Functions to expose into the VM
    static int snta_db_create(unqlite_context *pCtx, int argc, unqlite_value **argv);
    static int snta_db_attach(unqlite_context *pCtx, int argc, unqlite_value **argv);
    static int snta_db_detach(unqlite_context *pCtx, int argc, unqlite_value **argv);
    static int snta_db_destroy(unqlite_context *pCtx, int argc, unqlite_value **argv);

    static int sntd_coll_create(unqlite_context *pCtx, int argc, unqlite_value **argv);
    static int sntd_coll_exists(unqlite_context *pCtx, int argc, unqlite_value **argv);
    static int sntd_coll_open(unqlite_context *pCtx, int argc, unqlite_value **argv);
    static int sntd_coll_drop(unqlite_context *pCtx, int argc, unqlite_value **argv);
    static int sntd_execute(unqlite_context *pCtx, int argc, unqlite_value **argv);

    static int sntc_store(unqlite_context *pCtx, int argc, unqlite_value **argv);
    static int sntc_fetch(unqlite_context *pCtx, int argc, unqlite_value **argv);
    static int sntc_filter(unqlite_context *pCtx, int argc, unqlite_value **argv);
    static int sntc_update(unqlite_context *pCtx, int argc, unqlite_value **argv);
    static int sntc_all(unqlite_context *pCtx, int argc, unqlite_value **argv);
    static int sntc_last_record_id(unqlite_context *pCtx, int argc, unqlite_value **argv);
    static int sntc_size(unqlite_context *pCtx, int argc, unqlite_value **argv);
    static int sntc_erase(unqlite_context *pCtx, int argc, unqlite_value **argv);

    static int sntr_wait(unqlite_context *pCtx, int argc, unqlite_value **argv);
    static int sntr_test(unqlite_context *pCtx, int argc, unqlite_value **argv);
};

}

#endif
