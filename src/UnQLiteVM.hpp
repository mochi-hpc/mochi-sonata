/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_UNQLITE_VM_HPP
#define __SONATA_UNQLITE_VM_HPP

#include "UnQLiteValue.hpp"
#include <type_traits>
#include <string>
#include <spdlog/spdlog.h>

namespace sonata {

class UnQLiteBackend;

using namespace std::string_literals;

class UnQLiteVM {

    public:

    UnQLiteVM(unqlite* database, const char* code, UnQLiteBackend* backend)
    : m_code(code)
    , m_db(database)
    , m_backend(backend) {
        compile();
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
        if(value == nullptr) {
            return UnQLiteValue(UnQLiteValue::Null(), m_vm);
        }
        return UnQLiteValue(value, m_vm, nullptr);
    }

    void registerSonataFunctions() {
        int ret;
        ret = unqlite_create_constant(m_vm, "__SCRIPT__", get_script, static_cast<void*>(this));
        // Admin functions
        ret = unqlite_create_function(m_vm, "snta_db_create",      snta_db_create,      static_cast<void*>(this));
        ret = unqlite_create_function(m_vm, "snta_db_attach",      snta_db_attach,      static_cast<void*>(this));
        ret = unqlite_create_function(m_vm, "snta_db_detach",      snta_db_detach,      static_cast<void*>(this));
        ret = unqlite_create_function(m_vm, "snta_db_destroy",     snta_db_destroy,     static_cast<void*>(this));
        // Database functions
        ret = unqlite_create_function(m_vm, "sntd_coll_create",    sntd_coll_create,    static_cast<void*>(this));
        ret = unqlite_create_function(m_vm, "sntd_coll_exists",    sntd_coll_exists,    static_cast<void*>(this));
        ret = unqlite_create_function(m_vm, "sntd_coll_open",      sntd_coll_open,      static_cast<void*>(this));
        ret = unqlite_create_function(m_vm, "sntd_coll_drop",      sntd_coll_drop,      static_cast<void*>(this));
        ret = unqlite_create_function(m_vm, "sntd_execute",        sntd_execute,        static_cast<void*>(this));
        // Collection functions
        ret = unqlite_create_function(m_vm, "sntc_store",          sntc_store,          static_cast<void*>(this));
        ret = unqlite_create_function(m_vm, "sntc_fetch",          sntc_fetch,          static_cast<void*>(this));
        ret = unqlite_create_function(m_vm, "sntc_filter",         sntc_filter,         static_cast<void*>(this));
        ret = unqlite_create_function(m_vm, "sntc_update",         sntc_update,         static_cast<void*>(this));
        ret = unqlite_create_function(m_vm, "sntc_all",            sntc_all,            static_cast<void*>(this));
        ret = unqlite_create_function(m_vm, "sntc_last_record_id", sntc_last_record_id, static_cast<void*>(this));
        ret = unqlite_create_function(m_vm, "sntc_size",           sntc_size,           static_cast<void*>(this));
        ret = unqlite_create_function(m_vm, "sntc_erase",          sntc_erase,          static_cast<void*>(this));
        // Wait functions
        ret = unqlite_create_function(m_vm, "sntr_wait",           sntr_wait,           static_cast<void*>(this));
        ret = unqlite_create_function(m_vm, "sntr_test",           sntr_test,           static_cast<void*>(this));
    }

    private:

    const char*     m_code    = nullptr;
    unqlite*        m_db      = nullptr;
    unqlite_vm*     m_vm      = nullptr;
    UnQLiteBackend* m_backend = nullptr;
    std::vector<std::unique_ptr<std::string>> m_encoded_names;

    void compile() {
        int ret = unqlite_compile(m_db, m_code, strlen(m_code), &m_vm);
        if(ret != UNQLITE_OK) parse_and_throw_error();
    }

    std::string extract_function_code(const char* function_name, bool include_fun_decl=true) {
        const char* tmp = m_code;
        const char* function_beginning = nullptr;
        const char* function_body_beginning = nullptr;
        size_t fun_name_len = strlen(function_name);
        while((tmp = strstr(tmp, "function")) != NULL) {
            function_beginning = tmp;
            // check if the previous characted is not alphanum (i.e. the "function"
            // substring found would be part of a name, like "myfunction"
            if(tmp != m_code) {
                if(isalpha(*(tmp-1)) || *(tmp-1) == '_') {
                    tmp += 1;
                    continue;
                }
            }
            // check if the next characted is not alphanum (i.e. the "function"
            // substring found would be part of a name, like "functionbla"
            if(isalpha(*(tmp+8)) || *(tmp+8) == '_' || *(tmp+8) == '\0') {
                tmp += 1;
                continue;
            }
            // ok we know "function" was indeed used as a keyword
            tmp += 8; // get passed the "function" keyword
            // get passed non-alphabetical characters
            while(!(isalpha(*tmp) || *tmp == '_' || *tmp == '\0'))
                tmp += 1;
            if(strncmp(tmp, function_name, fun_name_len) != 0) {
                continue; // function name doesn't match, continue
            }
            if(isalnum(*(tmp+fun_name_len)) || *(tmp+fun_name_len) == '_')
                continue; // the function name is longer than we are looking for
            // of we know we passed the function's name
            // find the '{' character
            while(*tmp != '{') tmp += 1;
            function_body_beginning = tmp+1;
            unsigned b = 1; // track opened {
            do {
                tmp += 1;
                if(*tmp == '{') b += 1;
                if(*tmp == '}') b -= 1;
                if(*tmp == '\0') break;
            } while(b != 0);
            if(*tmp == '\0') return std::string();
            const char* function_body_end = tmp;
            const char* function_end = tmp+1;
            if(include_fun_decl) {
                return std::string(function_beginning, (size_t)(function_end-function_beginning));
            } else {
                return std::string(function_body_beginning, (size_t)(function_body_end-function_body_beginning));
            }
        }
        return std::string();
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
    static void get_script(unqlite_value *pValue, void *pUserData);
    
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
