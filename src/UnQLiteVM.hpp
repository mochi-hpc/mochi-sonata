#ifndef __SONATA_UNQLITE_VM_HPP
#define __SONATA_UNQLITE_VM_HPP

#include "UnQLiteValue.hpp"

#include <type_traits>
#include <string>

namespace sonata {

using namespace std::string_literals;

class UnQLiteVM {

    public:

    UnQLiteVM(unqlite* database, const char* code)
    : m_code(code)
    , m_db(database) {
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
        return UnQLiteValue(value, m_vm).as<T>();
    }

    UnQLiteValue operator[](const std::string& name) const {
        unqlite_value* value = unqlite_vm_extract_variable(m_vm, name.c_str());
        return UnQLiteValue(value, m_vm);
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

    void parse_and_throw_error() {
        const char *errorBuffer;
        int len;
        unqlite_config(m_db, UNQLITE_CONFIG_JX9_ERR_LOG, &errorBuffer, &len);
        std::string error = "UnQLite error: "s;
        if(len > 0) {
            error += errorBuffer;
            throw std::runtime_error(error);
        }
        unqlite_config(m_db, UNQLITE_CONFIG_ERR_LOG, &errorBuffer, &len);
        if(len > 0) {
            error += errorBuffer;
            throw std::runtime_error(error);
        }
        error += "(unknown)";
        throw std::runtime_error(error);
    }

    static int output_callback(const void *pOutput, unsigned int nOutputLen, void *pUserData) {
        auto function_ptr = static_cast<std::function<int(const void* , unsigned in)>*>(pUserData);
        if(function_ptr) {
            return (*function_ptr)(pOutput, nOutputLen);
        } else {
            return UNQLITE_OK;
        }
    }
};

}

#endif
