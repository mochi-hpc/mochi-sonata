/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_JSON_VALUE_SERIALIZE_HPP
#define __SONATA_JSON_VALUE_SERIALIZE_HPP

#include <sonata/Exception.hpp>
#include <nlohmann/json.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/tuple.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/serialization/stl/map.hpp>

namespace sonata {

using nlohmann::json;

struct JsonWrapper {
    json m_object;
    JsonWrapper() = default;
    ~JsonWrapper() = default;
    JsonWrapper(const JsonWrapper&) = default;
    JsonWrapper(JsonWrapper&&) = default;
    JsonWrapper& operator=(const JsonWrapper&) = default;
    JsonWrapper& operator=(JsonWrapper&&) = default;

    const json* operator->() const {
        return &m_object;
    }
    json* operator->() {
        return &m_object;
    }
    JsonWrapper& operator=(const json& obj) {
        m_object = obj;
        return *this;
    }
    JsonWrapper& operator=(json&& obj) {
        m_object = std::move(obj);
        return *this;
    }
};

struct JsonRefWrapper {
    json& m_object;
    ~JsonRefWrapper() = default;
    JsonRefWrapper(json& obj) : m_object(obj) {}
    JsonRefWrapper(const JsonRefWrapper&) = default;
    JsonRefWrapper(JsonRefWrapper&&) = default;
    JsonRefWrapper& operator=(const JsonRefWrapper&) = default;
    JsonRefWrapper& operator=(JsonRefWrapper&&) = default;

    const json* operator->() const {
        return &m_object;
    }
    json* operator->() {
        return &m_object;
    }
    JsonRefWrapper& operator=(const json& obj) {
        m_object = obj;
        return *this;
    }
    JsonRefWrapper& operator=(json&& obj) {
        m_object = std::move(obj);
        return *this;
    }
};

struct ConstJsonRefWrapper {
    const json& m_object;
    ~ConstJsonRefWrapper() = default;
    ConstJsonRefWrapper(const json& obj) : m_object(obj) {}
    ConstJsonRefWrapper(const ConstJsonRefWrapper&) = default;
    ConstJsonRefWrapper(ConstJsonRefWrapper&&) = default;
    ConstJsonRefWrapper& operator=(const ConstJsonRefWrapper&) = default;
    ConstJsonRefWrapper& operator=(ConstJsonRefWrapper&&) = default;

    const json* operator->() const {
        return &m_object;
    }
};

template <typename A> void saveJSON(A &ar, json const &val) {
  ar((char)val.type());
  switch (val.type()) {
  case json::value_t::null:
    break;
  case json::value_t::number_integer:
    ar(val.get<json::number_integer_t>());
    break;
  case json::value_t::number_unsigned:
    ar(val.get<json::number_unsigned_t>());
    break;
  case json::value_t::number_float:
    ar(val.get<json::number_float_t>());
    break;
  case json::value_t::string:
    ar(val.get_ref<const json::string_t&>());
    break;
  case json::value_t::boolean:
    ar(val.get<json::boolean_t>());
    break;
  case json::value_t::array:
    ar((size_t)val.size());
    for(auto& v : val) {
        saveJSON(ar, v);
    }
    break;
  case json::value_t::object:
    ar((size_t)val.size());
    for(auto& e : val.items()) {
        ar((std::string)e.key());
        saveJSON(ar, e.value());
    }
    break;
  case json::value_t::binary:
  case json::value_t::discarded:
    throw sonata::Exception("Invalid json type found (binary or discarded)");
  }
}

template <typename A> void loadJSON(A &ar, json &val) {
  char t;
  ar(t);
  switch ((json::value_t)t) {
  case json::value_t::null:
    break;
  case json::value_t::number_integer:
    {
        json::number_integer_t v;
        ar(v);
        val = v;
    }
    break;
  case json::value_t::number_unsigned:
    {
        json::number_unsigned_t v;
        ar(v);
        val = v;
    }
    break;
  case json::value_t::number_float:
    {
        json::number_float_t v;
        ar(v);
        val = v;
    }
    break;
  case json::value_t::string:
    {
        json::string_t v;
        ar(v);
        val = std::move(v);
    }
    break;
  case json::value_t::boolean:
    {
        json::boolean_t v;
        ar(v);
        val = v;
    }
    break;
  case json::value_t::array:
    {
        size_t s;
        val = json::array();
        ar(s);
        for(size_t i=0; i < s; i++) {
            JsonWrapper w;
            ar(w);
            val.push_back(std::move(w.m_object));
        }
    }
    break;
  case json::value_t::object:
    {
        size_t s;
        val = json::object();
        ar(s);
        for(size_t i=0; i < s; i++) {
            std::string key;
            JsonWrapper w;
            ar(key);
            ar(w);
            val.emplace(key, std::move(w.m_object));
        }
    }
    break;
  case json::value_t::binary:
  case json::value_t::discarded:
    throw sonata::Exception("Invalid json type found (binary or discarded)");
  }
}

template <typename A> void load(A &ar, JsonWrapper& wrapper) {
    loadJSON(ar, wrapper.m_object);
}

template <typename A> void load(A &ar, JsonRefWrapper& wrapper) {
    loadJSON(ar, wrapper.m_object);
}

template <typename A> void load(A& ar, ConstJsonRefWrapper& wrapper) {
    loadJSON(ar, const_cast<json&>(wrapper.m_object));
}

template <typename A> void save(A& ar, const JsonWrapper& wrapper) {
    saveJSON(ar, wrapper.m_object);
}

template <typename A> void save(A& ar, const JsonRefWrapper& wrapper) {
    saveJSON(ar, wrapper.m_object);
}

template <typename A> void save(A& ar, const ConstJsonRefWrapper& wrapper) {
    saveJSON(ar, wrapper.m_object);
}

} // namespace sonata

#endif
