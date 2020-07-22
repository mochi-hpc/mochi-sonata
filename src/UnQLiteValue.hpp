/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_UNQLITE_VALUE_HPP
#define __SONATA_UNQLITE_VALUE_HPP

#include "unqlite/unqlite.h"
#include <type_traits>
#include <string>
#include <vector>
#include <map>
#include <cstring>
#include <unordered_map>
#include <iostream>
#include <json/json.h>
#include <spdlog/spdlog.h>

#include "sonata/Exception.hpp"
#include "invoke/invoke.hpp"

namespace sonata {

class UnQLiteVM;

class UnQLiteValue {

    private:

    friend class UnQLiteVM;

    template<typename T>
    struct type {};

    unqlite_value*   m_value = nullptr;
    unqlite_vm*      m_vm    = nullptr;
    unqlite_context* m_ctx   = nullptr;
    bool             m_ref   = false;

    public:

    UnQLiteValue(unqlite_value* val, unqlite_vm* vm, unqlite_context* ctx, bool is_ref = false)
    : m_value(val)
    , m_vm(vm)
    , m_ctx(ctx)
    , m_ref(is_ref)
    {}

    struct Null {};

    UnQLiteValue(UnQLiteValue&& other)
    : m_vm(other.m_vm)
    , m_value(other.m_value)
    , m_ctx(other.m_ctx) 
    {
        other.m_vm = nullptr;
        other.m_value = nullptr;
        other.m_ctx = nullptr;
    }

    UnQLiteValue(const UnQLiteValue& other) = delete;

    UnQLiteValue& operator=(UnQLiteValue&& other) {
        if(&other == this) return *this;
        if(m_value && m_vm) {
            unqlite_vm_release_value(m_vm, m_value);
        } else
        if(m_value && m_ctx) {
            unqlite_context_release_value(m_ctx, m_value);
        }
        m_vm = other.m_vm;
        m_value = other.m_value;
        m_ctx = other.m_ctx;
        other.m_vm = nullptr;
        other.m_value = nullptr;
        other.m_ctx = nullptr;
        return *this;
    }

    UnQLiteValue& operator=(const UnQLiteValue& other) = delete;

    UnQLiteValue(unqlite_vm* vm)
    : m_vm(vm)
    , m_value(unqlite_vm_new_scalar(vm)) {
        unqlite_value_null(m_value);
    }

    UnQLiteValue(unqlite_context* ctx)
    : m_ctx(ctx)
    , m_value(unqlite_context_new_scalar(ctx)) {
        unqlite_value_null(m_value);
    }

    UnQLiteValue(const Json::Value& val, unqlite_vm* vm)
    : m_vm(vm) {
        switch(val.type()) {
        case Json::nullValue:
            m_value = unqlite_vm_new_scalar(vm);
            unqlite_value_null(m_value);
            break;
        case Json::intValue:
            m_value = unqlite_vm_new_scalar(vm);
            unqlite_value_int64(m_value, val.asInt64());
            break;
        case Json::uintValue:
            m_value = unqlite_vm_new_scalar(vm);
            unqlite_value_int64(m_value, val.asInt64());
            break;
        case Json::realValue:
            m_value = unqlite_vm_new_scalar(vm);
            unqlite_value_double(m_value, val.asDouble());
            break;
        case Json::stringValue:
            m_value = unqlite_vm_new_scalar(vm);
            unqlite_value_string(m_value, val.asString().c_str(), -1);
            break;
        case Json::booleanValue:
            m_value = unqlite_vm_new_scalar(vm);
            unqlite_value_bool(m_value, val.asBool());
            break;
        case Json::arrayValue:
            m_value = unqlite_vm_new_array(vm);
            for(unsigned i=0; i < val.size(); i++) {
                UnQLiteValue index(i, vm);
                UnQLiteValue element(val[i], vm);
                unqlite_array_add_elem(m_value, index.m_value, element.m_value);
            }
            break;
        case Json::objectValue:
            m_value = unqlite_vm_new_array(vm);
            for(auto it = val.begin(); it != val.end(); it++) {
                UnQLiteValue element(*it, vm);
                unqlite_array_add_strkey_elem(m_value, it.name().c_str(), element.m_value);
            }
            break; 
        }
    }

    UnQLiteValue(const Json::Value& val, unqlite_context* ctx)
    : m_ctx(ctx) {
        switch(val.type()) {
        case Json::nullValue:
            m_value = unqlite_context_new_scalar(ctx);
            unqlite_value_null(m_value);
            break;
        case Json::intValue:
            m_value = unqlite_context_new_scalar(ctx);
            unqlite_value_int64(m_value, val.asInt64());
            break;
        case Json::uintValue:
            m_value = unqlite_context_new_scalar(ctx);
            unqlite_value_int64(m_value, val.asInt64());
            break;
        case Json::realValue:
            m_value = unqlite_context_new_scalar(ctx);
            unqlite_value_double(m_value, val.asDouble());
            break;
        case Json::stringValue:
            m_value = unqlite_context_new_scalar(ctx);
            unqlite_value_string(m_value, val.asString().c_str(), -1);
            break;
        case Json::booleanValue:
            m_value = unqlite_context_new_scalar(ctx);
            unqlite_value_bool(m_value, val.asBool());
            break;
        case Json::arrayValue:
            m_value = unqlite_context_new_array(ctx);
            for(unsigned i=0; i < val.size(); i++) {
                UnQLiteValue index(i, ctx);
                UnQLiteValue element(val[i], ctx);
                unqlite_array_add_elem(m_value, index.m_value, element.m_value);
            }
            break;
        case Json::objectValue:
            m_value = unqlite_context_new_array(ctx);
            for(auto it = val.begin(); it != val.end(); it++) {
                UnQLiteValue element(*it, ctx);
                unqlite_array_add_strkey_elem(m_value, it.name().c_str(), element.m_value);
            }
            break; 
        }
    }

    UnQLiteValue(const Null&, unqlite_vm* vm)
    : m_vm(vm)
    , m_value(unqlite_vm_new_scalar(vm)) {
        unqlite_value_null(m_value);
    }

    UnQLiteValue(const Null&, unqlite_context* ctx)
    : m_ctx(ctx)
    , m_value(unqlite_context_new_scalar(ctx)) {
        unqlite_value_null(m_value);
    }

    template<typename IntegerType,
             std::enable_if_t<std::is_integral<IntegerType>::value, int> = 0>
    UnQLiteValue(const IntegerType& val, unqlite_vm* vm) 
    : m_vm(vm) 
    , m_value(unqlite_vm_new_scalar(vm)) {
        if(sizeof(IntegerType) <= 4) {
            unqlite_value_int(m_value, val);
        } else {
            unqlite_value_int64(m_value, val);
        }
    }

    template<typename IntegerType,
             std::enable_if_t<std::is_integral<IntegerType>::value, int> = 0>
    UnQLiteValue(const IntegerType& val, unqlite_context* ctx) 
    : m_ctx(ctx) 
    , m_value(unqlite_context_new_scalar(ctx)) {
        if(sizeof(IntegerType) <= 4) {
            unqlite_value_int(m_value, val);
        } else {
            unqlite_value_int64(m_value, val);
        }
    }

    template <typename FloatType,
              std::enable_if_t<std::is_floating_point<FloatType>::value, int> = 0>
    UnQLiteValue(const FloatType& val, unqlite_vm* vm) 
    : m_vm(vm) 
    , m_value(unqlite_vm_new_scalar(vm)) {
        unqlite_value_double(m_value, val);
    }

    template <typename FloatType,
              std::enable_if_t<std::is_floating_point<FloatType>::value, int> = 0>
    UnQLiteValue(const FloatType& val, unqlite_context* ctx) 
    : m_ctx(ctx) 
    , m_value(unqlite_context_new_scalar(ctx)) {
        unqlite_value_double(m_value, val);
    }

    UnQLiteValue(const bool& val, unqlite_vm* vm)
    : m_vm(vm) 
    , m_value(unqlite_vm_new_scalar(vm)) {
        unqlite_value_bool(m_value, val);
    }

    UnQLiteValue(const bool& val, unqlite_context* ctx)
    : m_ctx(ctx) 
    , m_value(unqlite_context_new_scalar(ctx)) {
        unqlite_value_bool(m_value, val);
    }

    UnQLiteValue(const std::string& str, unqlite_vm* vm)
    : m_vm(vm) 
    , m_value(unqlite_vm_new_scalar(vm)) {
        unqlite_value_string(m_value, str.c_str(), str.size());
    }

    UnQLiteValue(const std::string& str, unqlite_context* ctx)
    : m_ctx(ctx) 
    , m_value(unqlite_context_new_scalar(ctx)) {
        unqlite_value_string(m_value, str.c_str(), str.size());
    }

    UnQLiteValue(const char* str, unqlite_vm* vm)
    : m_vm(vm) 
    , m_value(unqlite_vm_new_scalar(vm)) {
        unqlite_value_string(m_value, str, strlen(str));
    }

    UnQLiteValue(const char* str, unqlite_context* ctx)
    : m_ctx(ctx) 
    , m_value(unqlite_context_new_scalar(ctx)) {
        unqlite_value_string(m_value, str, strlen(str));
    }

    template<typename T>
    UnQLiteValue(const std::vector<T>& vec, unqlite_vm* vm)
    : m_vm(vm) 
    , m_value(unqlite_vm_new_array(vm)) {
        for(const auto& val : vec) {
            UnQLiteValue uval(val, vm);
            unqlite_array_add_elem(m_value, nullptr, uval.m_value);
        }
    }

    template<typename T>
    UnQLiteValue(const std::vector<T>& vec, unqlite_context* ctx)
    : m_ctx(ctx) 
    , m_value(unqlite_context_new_array(ctx)) {
        for(const auto& val : vec) {
            UnQLiteValue uval(val, ctx);
            unqlite_array_add_elem(m_value, nullptr, uval.m_value);
        }
    }

    UnQLiteValue(const std::vector<UnQLiteValue>& vec, unqlite_vm* vm)
    : m_vm(vm) 
    , m_value(unqlite_vm_new_array(vm)) {
        for(const auto& val : vec) {
            unqlite_array_add_elem(m_value, nullptr, val.m_value);
        }
    }

    UnQLiteValue(const std::vector<UnQLiteValue>& vec, unqlite_context* ctx)
    : m_ctx(ctx) 
    , m_value(unqlite_context_new_array(ctx)) {
        for(const auto& val : vec) {
            unqlite_array_add_elem(m_value, nullptr, val.m_value);
        }
    }

    template<typename T, size_t N>
    UnQLiteValue(const std::array<T,N>& vec, unqlite_vm* vm)
    : m_vm(vm) 
    , m_value(unqlite_vm_new_array(vm)) {
        for(const auto& val : vec) {
            UnQLiteValue uval(val, vm);
            unqlite_array_add_elem(m_value, nullptr, uval.m_value);
        }
    }

    template<typename T, size_t N>
    UnQLiteValue(const std::array<T,N>& vec, unqlite_context* ctx)
    : m_ctx(ctx) 
    , m_value(unqlite_context_new_array(ctx)) {
        for(const auto& val : vec) {
            UnQLiteValue uval(val, ctx);
            unqlite_array_add_elem(m_value, nullptr, uval.m_value);
        }
    }

    template<size_t N>
    UnQLiteValue(const std::array<UnQLiteValue,N>& vec, unqlite_vm* vm)
    : m_vm(vm) 
    , m_value(unqlite_vm_new_array(vm)) {
        for(const auto& val : vec) {
            unqlite_array_add_elem(m_value, nullptr, val.m_value);
        }
    }

    template<size_t N>
    UnQLiteValue(const std::array<UnQLiteValue,N>& vec, unqlite_context* ctx)
    : m_ctx(ctx) 
    , m_value(unqlite_context_new_array(ctx)) {
        for(const auto& val : vec) {
            unqlite_array_add_elem(m_value, nullptr, val.m_value);
        }
    }

    UnQLiteValue(const std::pair<UnQLiteValue,UnQLiteValue>& p, unqlite_vm* vm)
    : m_vm(vm)
    , m_value(unqlite_vm_new_array(vm)) {
        unqlite_array_add_elem(m_value, nullptr, p.first.m_value);
        unqlite_array_add_elem(m_value, nullptr, p.second.m_value);
    }

    UnQLiteValue(const std::pair<UnQLiteValue,UnQLiteValue>& p, unqlite_context* ctx)
    : m_ctx(ctx)
    , m_value(unqlite_context_new_array(ctx)) {
        unqlite_array_add_elem(m_value, nullptr, p.first.m_value);
        unqlite_array_add_elem(m_value, nullptr, p.second.m_value);
    }

    template<typename T>
    UnQLiteValue(const std::map<std::string,T>& values, unqlite_vm* vm)
    : m_vm(vm) 
    , m_value(unqlite_vm_new_array(vm)) {
        for(const auto& p : values) {
            UnQLiteValue uval(p.second, vm);
            unqlite_array_add_strkey_elem(m_value, p.first.c_str(), uval.m_value);
        }
    }

    template<typename T>
    UnQLiteValue(const std::map<std::string,T>& values, unqlite_context* ctx)
    : m_ctx(ctx) 
    , m_value(unqlite_context_new_array(ctx)) {
        for(const auto& p : values) {
            UnQLiteValue uval(p.second, ctx);
            unqlite_array_add_strkey_elem(m_value, p.first.c_str(), uval.m_value);
        }
    }

    template<typename T>
    UnQLiteValue(const std::unordered_map<std::string,T>& values, unqlite_vm* vm)
    : m_vm(vm) 
    , m_value(unqlite_vm_new_array(vm)) {
        for(const auto& p : values) {
            UnQLiteValue uval(p.second, vm);
            unqlite_array_add_strkey_elem(m_value, p.first.c_str(), uval.m_value);
        }
    }

    template<typename T>
    UnQLiteValue(const std::unordered_map<std::string,T>& values, unqlite_context* ctx)
    : m_ctx(ctx) 
    , m_value(unqlite_context_new_array(ctx)) {
        for(const auto& p : values) {
            UnQLiteValue uval(p.second, ctx);
            unqlite_array_add_strkey_elem(m_value, p.first.c_str(), uval.m_value);
        }
    }

    template<typename T>
    UnQLiteValue(const std::map<std::string,UnQLiteValue>& values, unqlite_vm* vm)
    : m_vm(vm) 
    , m_value(unqlite_vm_new_array(vm)) {
        for(const auto& p : values) {
            unqlite_array_add_strkey_elem(m_value, p.first.c_str(), p.second.m_value);
        }
    }

    template<typename T>
    UnQLiteValue(const std::map<std::string,UnQLiteValue>& values, unqlite_context* ctx)
    : m_ctx(ctx) 
    , m_value(unqlite_context_new_array(ctx)) {
        for(const auto& p : values) {
            unqlite_array_add_strkey_elem(m_value, p.first.c_str(), p.second.m_value);
        }
    }

    template<typename T>
    UnQLiteValue(const std::unordered_map<std::string,UnQLiteValue>& values, unqlite_vm* vm)
    : m_vm(vm) 
    , m_value(unqlite_vm_new_array(vm)) {
        for(const auto& p : values) {
            unqlite_array_add_strkey_elem(m_value, p.first.c_str(), p.second.m_value);
        }
    }

    template<typename T>
    UnQLiteValue(const std::unordered_map<std::string,UnQLiteValue>& values, unqlite_context* ctx)
    : m_ctx(ctx) 
    , m_value(unqlite_context_new_array(ctx)) {
        for(const auto& p : values) {
            unqlite_array_add_strkey_elem(m_value, p.first.c_str(), p.second.m_value);
        }
    }

    ~UnQLiteValue() {
        if(m_ref) return;
        if(m_vm && m_value) {
            unqlite_vm_release_value(m_vm, m_value);
        }
        else if(m_ctx && m_value) {
            unqlite_context_release_value(m_ctx, m_value);
        }
    }

    size_t size() const {
        return unqlite_array_count(m_value);
    }

    UnQLiteValue operator[](int index) const {
        if(index < 0) {
            index = size()-index;
        }
        auto index_str = std::to_string(index);
        unqlite_value* result = unqlite_array_fetch(m_value, index_str.c_str(), -1);
        if(result) {
            return UnQLiteValue(result, m_vm, m_ctx);
        } else {
            if(m_vm) return UnQLiteValue(Null(), m_vm);
            else     return UnQLiteValue(Null(), m_ctx);
        }
    }

    UnQLiteValue operator[](const std::string& key) const {
        unqlite_value* result = unqlite_array_fetch(m_value, key.c_str(), -1);
        if(result) {
            return UnQLiteValue(result, m_vm, m_ctx);
        } else {
            if(m_vm) return UnQLiteValue(Null(), m_vm);
            else     return UnQLiteValue(Null(), m_ctx);
        }
    }

    UnQLiteValue operator[](const char* key) const {
        unqlite_value* result = unqlite_array_fetch(m_value, key, -1);
        if(result) {
            return UnQLiteValue(result, m_vm, m_ctx);
        } else {
            if(m_vm) return UnQLiteValue(Null(), m_vm);
            else     return UnQLiteValue(Null(), m_ctx);
        }
    }

    template<typename T>
    T as() const {
        return convertType(m_value, type<T>());
    }

    template<typename T>
    bool is() const {
        return checkType(m_value, type<T>());
    }

    template<typename T>
    operator T() const {
        if(!is<T>()) {
            throw Exception("Invalid type conversion requested");
        }
        return as<T>();
    }

    bool operator<(const UnQLiteValue& other) const {
        return unqlite_value_compare(m_value, other.m_value, true) < 0;
    }
    
    bool operator>(const UnQLiteValue& other) const {
        return unqlite_value_compare(m_value, other.m_value, true) > 0;
    }

    bool operator<=(const UnQLiteValue& other) const {
        return unqlite_value_compare(m_value, other.m_value, true) <= 0;
    }
    
    bool operator>=(const UnQLiteValue& other) const {
        return unqlite_value_compare(m_value, other.m_value, true) >= 0;
    }

    bool operator==(const UnQLiteValue& other) const {
        return unqlite_value_compare(m_value, other.m_value, true) == 0;
    }

    bool operator!=(const UnQLiteValue& other) const {
        return unqlite_value_compare(m_value, other.m_value, true) != 0;
    }

    template<typename Stream>
    Stream& printToStream(Stream& os) const {
        if(is<std::string>()) {
            os << "\"" << unqlite_value_to_string(m_value, nullptr) << "\"";
        } else {
            os << unqlite_value_to_string(m_value, nullptr);
        }

        return os;
    }

    static int foreach(unqlite_value *pArr, const std::function<int(unsigned, unqlite_value*)>& f) {
        if(not unqlite_value_is_json_array(pArr)) {
            throw Exception("UnQLiteValue is not a JSON array");
        }
        std::pair<unsigned, const std::function<int(unsigned, unqlite_value*)>*> args;
        args.first = 0;
        args.second = &f;
        return unqlite_array_walk(pArr, foreachArrayCallback,
                const_cast<void*>(reinterpret_cast<const void*>(&args)));
    }

    static int foreach(unqlite_value *pMap, const std::function<int(const std::string&, unqlite_value*)>& f) {
        if(not unqlite_value_is_json_object(pMap)) {
            throw Exception("UnQLiteValue is not a JSON object");
        }
        return unqlite_array_walk(pMap, foreachMapCallback,
                const_cast<void*>(reinterpret_cast<const void*>(&f)));
    }
    
    void foreach(const std::function<void(unsigned, const UnQLiteValue&)>& f) {
        foreach(m_value, [vm=m_vm, ctx=m_ctx, &f](unsigned i, unqlite_value* elem) {
            UnQLiteValue val(elem, vm, ctx, true);
            f(i, val);
            return UNQLITE_OK;
        });
    }

    void foreach(const std::function<void(const std::string&, const UnQLiteValue&)>& f) {
        foreach(m_value, [vm=m_vm, ctx=m_ctx, &f](const std::string& key, unqlite_value* elem) {
            UnQLiteValue val(elem, vm, ctx, true);
            f(key, val);
            return UNQLITE_OK;
        });
    }

    private:

    static int foreachArrayCallback(unqlite_value *pKey, unqlite_value *pValue, void *pUserData) {
        using arg_type = std::pair<unsigned, std::function<int(unsigned, unqlite_value*)>*>;
        auto args = reinterpret_cast<arg_type*>(pUserData);
        int ret = (*(args->second))(args->first, pValue);
        args->first += 1;
        return ret;
    }

    static int foreachMapCallback(unqlite_value *pKey, unqlite_value *pValue, void *pUserData) {
        using arg_type = std::function<int(const std::string&, unqlite_value*)>;
        auto f = reinterpret_cast<arg_type*>(pUserData);
        std::string key = unqlite_value_to_string(pKey, nullptr);
        return (*f)(key, pValue);
    }

    template<typename Stream>
    friend int printUnQLiteVec(unqlite_value *pKey, unqlite_value *pValue, void *pUserData);

    template<typename Stream>
    friend int printUnQLiteMap(unqlite_value *pKey, unqlite_value *pValue, void *pUserData);

    template<typename T1>
    void fillArrayFromTuple(T1&& x) {
        if(m_vm) {
            UnQLiteValue v(x, m_vm);
            unqlite_array_add_elem(m_value, nullptr, v.m_value);
        }
        else {
            UnQLiteValue v(x, m_ctx);
            unqlite_array_add_elem(m_value, nullptr, v.m_value);
        }
    }

    template<typename T1, typename ... Tn>
    void fillArrayFromTuple(T1&& x, Tn&&... xn) {
        fillArrayFromTyple(std::forward<T1>(x));
        fillArrayFromTuple(std::forward<Tn>(xn)...);
    }

    static Null convertType(unqlite_value* value, const type<Null>&) {
        return Null();
    }

    template<typename IntegerType,
             std::enable_if_t<std::is_integral<IntegerType>::value, int> = 0>
    static IntegerType convertType(unqlite_value* value, const type<IntegerType>&) {
        if(sizeof(IntegerType) > 4) {
            return unqlite_value_to_int64(value);
        } else {
            return unqlite_value_to_int(value);
        }
    }

    template<typename FloatType,
             std::enable_if_t<std::is_floating_point<FloatType>::value, int> = 0>
    static FloatType convertType(unqlite_value* value, const type<FloatType>&) {
        return unqlite_value_to_double(value);
    }

    static bool convertType(unqlite_value* value, const type<bool>&) {
        return unqlite_value_to_bool(value);
    }

    static std::string convertType(unqlite_value* value, const type<std::string>&) {
        return unqlite_value_to_string(value, NULL);
    }

    template<typename T>
    static std::vector<T> convertType(unqlite_value* value, const type<std::vector<T>>&) {
        std::vector<T> result;
        foreach(value, [&result](unsigned, unqlite_value *pValue) {
            result.push_back(UnQLiteValue::convertType(pValue, UnQLiteValue::type<T>()));
            return UNQLITE_OK;
        });
        return result;
    }

    template<typename T>
    static std::map<std::string, T> convertType(unqlite_value* value, const type<std::map<std::string, T>>&) {
        std::map<std::string, T> result;
        foreach(value, [&result](const std::string& key, unqlite_value* pValue) {
            result[key] = UnQLiteValue::convertType(pValue, UnQLiteValue::type<T>());
        });
        return result;
    }

    template<typename T>
    static std::unordered_map<std::string, T> convertType(unqlite_value* value, const type<std::unordered_map<std::string, T>>&) {
        std::unordered_map<std::string, T> result;
        foreach(value, [&result](const std::string& key, unqlite_value* pValue) {
            result[key] = UnQLiteValue::convertType(pValue, UnQLiteValue::type<T>());
        });
        return result;
    }

    static Json::Value convertType(unqlite_value* value, const type<Json::Value>&) {
        Json::Value result;
        if(unqlite_value_is_null(value)) {
        } else if(unqlite_value_is_int(value)) {
            result = (int64_t)unqlite_value_to_int64(value);
        } else if(unqlite_value_is_float(value)) {
            result = unqlite_value_to_double(value);
        } else if(unqlite_value_is_bool(value)) {
            result = (bool)unqlite_value_to_bool(value);
        } else if(unqlite_value_is_string(value)) {
            result = unqlite_value_to_string(value, nullptr);
        } else if(unqlite_value_is_json_object(value)) {
            foreach(value, [&result](const std::string& key, unqlite_value *pValue) {
                result[key] = convertType(pValue, type<Json::Value>());
                return UNQLITE_OK;
            });
        } else if(unqlite_value_is_json_array(value)) {
            foreach(value, [&result](unsigned, unqlite_value *pValue) {
                result.append(convertType(pValue, type<Json::Value>()));
                return UNQLITE_OK;
            });
        } else {
            throw Exception("Cannot convert UnQLiteValue into Json::Value");
        }
        return result;
    }

    static bool checkType(unqlite_value* value, const type<Null>&) {
        return unqlite_value_is_null(value);
    }

    static bool checkType(unqlite_value* value, const type<Json::Value>&) {
        return true;
    }

    template<typename IntegerType,
             std::enable_if_t<std::is_integral<IntegerType>::value, int> = 0>
    static bool checkType(unqlite_value* value, const type<IntegerType>&) {
        return unqlite_value_is_int(value);
    }

    template<typename FloatType,
             std::enable_if_t<std::is_floating_point<FloatType>::value, int> = 0>
    static bool checkType(unqlite_value* value, const type<FloatType>&) {
        return unqlite_value_is_float(value);
    }

    static bool checkType(unqlite_value* value, const type<bool>&) {
        return unqlite_value_is_bool(value);
    }

    static bool checkType(unqlite_value* value, const type<std::string>& ) {
        return unqlite_value_is_string(value);
    }

    template<typename T>
    static bool checkType(unqlite_value* value, const type<std::vector<T>>&) {
        bool b =  unqlite_value_is_json_array(value);
        if(!b) return false;
        foreach(value, [&b](unsigned, unqlite_value *pValue) {
            b = b && checkType(pValue, type<T>());
            return UNQLITE_OK;
        });
        return b;
    }

    template<typename T>
    static bool checkType(unqlite_value* value, const type<std::map<std::string, T>>&) {
        bool b = unqlite_value_is_json_object(value);
        if(!b) return false;
        foreach(value, [&b](const std::string&, unqlite_value *pValue) {
            b = b && checkType(pValue, type<T>());
            return UNQLITE_OK;
        });
        return b;
    }

    template<typename T>
    static bool checkType(unqlite_value* value, const type<std::unordered_map<std::string, T>>&) {
        bool b = unqlite_value_is_json_object(value);
        if(!b) return false;
        foreach(value, [&b](const std::string&, unqlite_value *pValue) {
            b = b && checkType(pValue, type<T>());
            return UNQLITE_OK;
        });
        return b;
    }
};

}

#endif
