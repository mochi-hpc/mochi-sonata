#ifndef __SONATA_UNQLITE_VALUE_HPP
#define __SONATA_UNQLITE_VALUE_HPP

#include <unqlite.h>
#include <type_traits>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <iostream>

#include "sonata/Exception.hpp"
#include "invoke/invoke.hpp"

namespace sonata {

class UnQLiteVM;

class UnQLiteValue {

    private:

    friend class UnQLiteVM;

    template<typename T>
    struct type {};

    unqlite_value* m_value = nullptr;
    unqlite_vm*    m_vm    = nullptr;

    UnQLiteValue(unqlite_value* val, unqlite_vm* vm)
    : m_value(val)
    , m_vm(vm) {}

    public:

    struct Null {};

    UnQLiteValue(UnQLiteValue&& other)
    : m_vm(other.m_vm)
    , m_value(other.m_value) {
        other.m_vm = nullptr;
        other.m_value = nullptr;
    }

    UnQLiteValue(const UnQLiteValue& other) = delete;

    UnQLiteValue& operator=(UnQLiteValue&& other) {
        if(&other == this) return *this;
        if(m_value && m_vm) {
            unqlite_vm_release_value(m_vm, m_value);
        }
        m_vm = other.m_vm;
        m_value = other.m_value;
        other.m_vm = nullptr;
        other.m_value = nullptr;
        return *this;
    }

    UnQLiteValue& operator=(const UnQLiteValue& other) = delete;

    UnQLiteValue(unqlite_vm* vm)
    : m_vm(vm)
    , m_value(unqlite_vm_new_scalar(vm)) {
        unqlite_value_null(m_value);
    }

    UnQLiteValue(const Null&, unqlite_vm* vm)
    : m_vm(vm)
    , m_value(unqlite_vm_new_scalar(vm)) {
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

    template <typename FloatType,
              std::enable_if_t<std::is_floating_point<FloatType>::value, int> = 0>
    UnQLiteValue(const FloatType& val, unqlite_vm* vm) 
    : m_vm(vm) 
    , m_value(unqlite_vm_new_scalar(vm)) {
        unqlite_value_double(m_value, val);
    }

    UnQLiteValue(const bool& val, unqlite_vm* vm)
    : m_vm(vm) 
    , m_value(unqlite_vm_new_scalar(vm)) {
        unqlite_value_bool(m_value, val);
    }

    UnQLiteValue(const std::string& str, unqlite_vm* vm)
    : m_vm(vm) 
    , m_value(unqlite_vm_new_scalar(vm)) {
        unqlite_value_string(m_value, str.c_str(), str.size());
    }

    UnQLiteValue(const char* str, unqlite_vm* vm)
    : m_vm(vm) 
    , m_value(unqlite_vm_new_scalar(vm)) {
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

    UnQLiteValue(const std::vector<UnQLiteValue>& vec, unqlite_vm* vm)
    : m_vm(vm) 
    , m_value(unqlite_vm_new_array(vm)) {
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

    template<size_t N>
    UnQLiteValue(const std::array<UnQLiteValue,N>& vec, unqlite_vm* vm)
    : m_vm(vm) 
    , m_value(unqlite_vm_new_array(vm)) {
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
    UnQLiteValue(const std::unordered_map<std::string,T>& values, unqlite_vm* vm)
    : m_vm(vm) 
    , m_value(unqlite_vm_new_array(vm)) {
        for(const auto& p : values) {
            UnQLiteValue uval(p.second, vm);
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
    UnQLiteValue(const std::unordered_map<std::string,UnQLiteValue>& values, unqlite_vm* vm)
    : m_vm(vm) 
    , m_value(unqlite_vm_new_array(vm)) {
        for(const auto& p : values) {
            unqlite_array_add_strkey_elem(m_value, p.first.c_str(), p.second.m_value);
        }
    }

    ~UnQLiteValue() {
        if(m_vm && m_value) {
            unqlite_vm_release_value(m_vm, m_value);
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
            return UnQLiteValue(result, m_vm);
        } else {
            return UnQLiteValue(Null(), m_vm);
        }
    }

    UnQLiteValue operator[](const std::string& key) const {
        unqlite_value* result = unqlite_array_fetch(m_value, key.c_str(), -1);
        if(result) {
            return UnQLiteValue(result, m_vm);
        } else {
            return UnQLiteValue(Null(), m_vm);
        }
    }

    UnQLiteValue operator[](const char* key) const {
        unqlite_value* result = unqlite_array_fetch(m_value, key, -1);
        if(result) {
            return UnQLiteValue(result, m_vm);
        } else {
            return UnQLiteValue(Null(), m_vm);
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
    Stream& printToStream(Stream& os) const;

    void foreach(const std::function<void(unsigned, const UnQLiteValue&)>& f) {
        if(not unqlite_value_is_json_array(m_value)) {
            throw Exception("UnQLiteValue is not an array");
        }
        using arg_type =
            std::tuple<size_t,
                   std::function<void(unsigned, const UnQLiteValue&)>*,
                   unqlite_vm*>;
        arg_type args;
        std::get<0>(args) = 0;
        std::get<1>(args) = const_cast<std::function<void(unsigned, const UnQLiteValue&)>*>(&f);
        std::get<2>(args) = m_vm;
        unqlite_array_walk(m_value, foreachArrayCallback,
                const_cast<void*>(reinterpret_cast<const void*>(&args)));
    }

    void foreach(const std::function<void(const std::string&, const UnQLiteValue&)>& f) {
        if(not unqlite_value_is_json_object(m_value)) {
            throw Exception("UnQLiteValue is not an array");
        }
        using arg_type =
            std::tuple<size_t,
                   std::function<void(const std::string&, const UnQLiteValue&)>*,
                   unqlite_vm*>;
        arg_type args;
        std::get<0>(args) = 0;
        std::get<1>(args) = const_cast<std::function<void(const std::string&, const UnQLiteValue&)>*>(&f);
        std::get<2>(args) = m_vm;
        unqlite_array_walk(m_value, foreachMapCallback,
                const_cast<void*>(reinterpret_cast<const void*>(&f)));
    }

    private:

    static int foreachArrayCallback(unqlite_value *pKey, unqlite_value *pValue, void *pUserData) {
        using arg_type =
            std::tuple<size_t,
                   std::function<void(unsigned, const UnQLiteValue&)>*,
                   unqlite_vm*>;
        auto p_args = reinterpret_cast<arg_type*>(pUserData);
        auto i = std::get<0>(*p_args);
        auto f = std::get<1>(*p_args);
        auto vm = std::get<2>(*p_args);
        UnQLiteValue val(pValue, vm);
        (*f)(i, val);
        val.m_vm = nullptr;
        val.m_value = nullptr;
        i += 1;
        return UNQLITE_OK;
    }

    static int foreachMapCallback(unqlite_value *pKey, unqlite_value *pValue, void *pUserData) {
        using arg_type =
            std::tuple<size_t,
                   std::function<void(const std::string&, const UnQLiteValue&)>*,
                   unqlite_vm*>;
        auto p_args = reinterpret_cast<arg_type*>(pUserData);
        auto& i = std::get<0>(*p_args);
        auto f = std::get<1>(*p_args);
        auto vm = std::get<2>(*p_args);
        UnQLiteValue key(pKey, vm);
        UnQLiteValue val(pValue, vm);
        (*f)(key, val);
        val.m_vm    = nullptr;
        val.m_value = nullptr;
        key.m_vm    = nullptr;
        key.m_value = nullptr;
        i += 1;
        return UNQLITE_OK;
    }

    template<typename Stream>
    friend int printUnQLiteVec(unqlite_value *pKey, unqlite_value *pValue, void *pUserData);

    template<typename Stream>
    friend int printUnQLiteMap(unqlite_value *pKey, unqlite_value *pValue, void *pUserData);

    template<typename T1>
    void fillArrayFromTuple(T1&& x) {
        UnQLiteValue v(x, m_vm);
        unqlite_array_add_elem(m_value, nullptr, v.m_value);
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
    static int fillArrayCallback(unqlite_value *pKey, unqlite_value *pValue, void *pUserData);

    template<typename T>
    static std::vector<T> convertType(unqlite_value* value, const type<std::vector<T>>&) {
        std::vector<T> result;
        unqlite_array_walk(value, fillArrayCallback<T>, &result);
        return result;
    }

    template<typename MapType>
    static int fillMapCallback(unqlite_value *pKey, unqlite_value *pValue, void *pUserData);

    template<typename T>
    static std::map<std::string, T> convertType(unqlite_value* value, const type<std::map<std::string, T>>&) {
        std::map<std::string, T> result;
        unqlite_array_walk(value, fillMapCallback<std::map<std::string,T>>, &result);
        return result;
    }

    template<typename T>
    static std::unordered_map<std::string, T> convertType(unqlite_value* value, const type<std::unordered_map<std::string, T>>&) {
        std::unordered_map<std::string, T> result;
        unqlite_array_walk(value, fillMapCallback<std::unordered_map<std::string,T>>, &result);
        return result;
    }

    static bool checkType(unqlite_value* value, const type<Null>&) {
        return unqlite_value_is_null(value);
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
    static int typeCheckerCallback(unqlite_value *pKey, unqlite_value *pValue, void *pUserData);

    template<typename T>
    static bool checkType(unqlite_value* value, const type<std::vector<T>>&) {
        bool b =  unqlite_value_is_json_array(value);
        if(!b) return false;
        unqlite_array_walk(value, typeCheckerCallback<T>, &b);
        return b;
    }

    template<typename T>
    static bool checkType(unqlite_value* value, const type<std::map<std::string, T>>&) {
        bool b = unqlite_value_is_json_object(value);
        if(!b) return false;
        unqlite_array_walk(value, typeCheckerCallback<T>, &b);
        return b;
    }

    template<typename T>
    static bool checkType(unqlite_value* value, const type<std::unordered_map<std::string, T>>&) {
        bool b = unqlite_value_is_json_object(value);
        if(!b) return false;
        unqlite_array_walk(value, typeCheckerCallback<T>, &b);
        return b;
    }
};
    
template<typename T>
int UnQLiteValue::typeCheckerCallback(unqlite_value *pKey, unqlite_value *pValue, void *pUserData) {
    bool* result = reinterpret_cast<bool*>(pUserData);
    *result = *result && UnQLiteValue::checkType(pValue, UnQLiteValue::type<T>());
    return UNQLITE_OK;
}
 
template<typename T>
int UnQLiteValue::fillArrayCallback(unqlite_value *pKey, unqlite_value *pValue, void *pUserData) {
    std::vector<T>* v = reinterpret_cast<std::vector<T>*>(pUserData);
    v->push_back(UnQLiteValue::convertType(pValue, UnQLiteValue::type<T>()));
    return UNQLITE_OK;
}

template<typename MapType>
int UnQLiteValue::fillMapCallback(unqlite_value *pKey, unqlite_value *pValue, void *pUserData) {
    MapType* m = reinterpret_cast<MapType*>(pUserData);
    std::string key = UnQLiteValue::convertType(pKey, UnQLiteValue::type<std::string>());
    (*m)[key] = UnQLiteValue::convertType(pValue, UnQLiteValue::type<typename MapType::mapped_type>());
    return UNQLITE_OK;
}

template<typename Stream>
struct UnQLitePrinterArgs {
    Stream*     stream;
    unqlite_vm* vm;
    size_t      count;
    size_t      current = 0;
};

template<typename Stream>
static int printUnQLiteMap(unqlite_value *pKey, unqlite_value *pValue, void *pUserData) {
    auto args = reinterpret_cast<UnQLitePrinterArgs<Stream>*>(pUserData);
    auto& os = *(args->stream);

    UnQLiteValue key(pKey, args->vm);
    UnQLiteValue val(pValue, args->vm);
    
    key.printToStream(os);
    os << " : ";
    val.printToStream(os);
    if(args->current < args->count-1)
        os << ",";
    os << " ";
    args->current += 1;

    return UNQLITE_OK;
}

template<typename Stream>
int printUnQLiteVec(unqlite_value *pKey, unqlite_value *pValue, void *pUserData) {
    auto args = reinterpret_cast<UnQLitePrinterArgs<Stream>*>(pUserData);
    auto& os = *(args->stream);

    UnQLiteValue val(pValue, args->vm);
    
    val.printToStream(os);
    if(args->current < args->count-1)
        os << ",";
    os << " ";
    args->current += 1;

    return UNQLITE_OK;
}

template<typename Stream>
Stream& UnQLiteValue::printToStream(Stream& os) const {
    if(is<int64_t>()) {
        os << as<int64_t>();
    } else if(is<double>()) {
        os << as<double>();
    } else if(is<bool>()) {
        bool b = as<bool>();
        if(b) os << "true";
        else os << "false";
    } else if(is<UnQLiteValue::Null>()) {
        os << "null";
    } else if(is<std::string>()) {
        os << "\"" << as<std::string>() << "\"";
    } else if(unqlite_value_is_json_array(m_value)
            && !unqlite_value_is_json_object(m_value)) {
        os << "[ ";
        UnQLitePrinterArgs<Stream> args;
        args.stream = &os;
        args.vm = m_vm;
        args.count = unqlite_array_count(m_value);
        unqlite_array_walk(m_value, printUnQLiteVec<Stream>, &args);
        os << "]";
    } else if(unqlite_value_is_json_object(m_value)) {
        os << "{ ";
        UnQLitePrinterArgs<Stream> args;
        args.stream = &os;
        args.vm = m_vm;
        args.count = unqlite_array_count(m_value);
        unqlite_array_walk(m_value, printUnQLiteMap<Stream>, &args);
        os << "}";
    } else if(unqlite_value_is_resource) {
        os << "\"resource@" << std::hex
           << (intptr_t)unqlite_value_to_resource(m_value) << "\"";
    }
    return os;
}

}


#endif
