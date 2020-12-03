/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __SONATA_JSON_VALUE_SERIALIZE_HPP
#define __SONATA_JSON_VALUE_SERIALIZE_HPP

#include <json/json.h>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/tuple.hpp>
#include <thallium/serialization/stl/vector.hpp>

namespace Json { // needed for template deduction to work

template <typename A> void save(A &ar, Json::Value const &val) {
  ar &(char)val.type();
  switch (val.type()) {
  case Json::nullValue:
    break;
  case Json::intValue:
    ar &val.asInt64();
    break;
  case Json::uintValue:
    ar &val.asUInt64();
    break;
  case Json::realValue:
    ar &val.asDouble();
    break;
  case Json::stringValue:
    ar &val.asString();
    break;
  case Json::booleanValue:
    ar &val.asBool();
    break;
  case Json::arrayValue:
    ar &val.size();
    for (unsigned i = 0; i < val.size(); i++) {
      ar &val[i];
    }
    break;
  case Json::objectValue:
    ar &val.size();
    for (auto it = val.begin(); it != val.end(); it++) {
      ar &it.name();
      ar &*it;
    }
    break;
  }
}

template <typename A> void load(A &ar, Json::Value &val) {
  char t;
  ar &t;
  switch ((Json::ValueType)t) {
  case Json::nullValue:
    val = Json::Value();
    break;
  case Json::intValue: {
    int64_t v;
    ar &v;
    val = v;
  } break;
  case Json::uintValue: {
    uint64_t v;
    ar &v;
    val = v;
  } break;
  case Json::realValue: {
    double v;
    ar &v;
    val = v;
  } break;
  case Json::stringValue: {
    std::string v;
    ar &v;
    val = std::move(v);
  } break;
  case Json::booleanValue: {
    bool v;
    ar &v;
    val = v;
  } break;
  case Json::arrayValue: {
    Json::ArrayIndex size;
    ar &size;
    val = Json::Value();
    for (unsigned i = 0; i < size; i++) {
      ar &val[i];
    }
  } break;
  case Json::objectValue: {
    Json::ArrayIndex size;
    ar &size;
    val = Json::Value();
    for (unsigned i = 0; i < size; i++) {
      std::string key;
      ar &key;
      ar &val[key];
    }
  } break;
  }
}

} // namespace Json

#endif
