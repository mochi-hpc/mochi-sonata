#include "UnQLiteValue.hpp"

namespace sonata {

int UnQLiteValue::fillJsonValueMapCallback(unqlite_value *pKey, unqlite_value *pValue, void *pUserData) {
    Json::Value* result = reinterpret_cast<Json::Value*>(pUserData);
    Json::Value item = UnQLiteValue::convertType(pValue, UnQLiteValue::type<Json::Value>());
    const char* key = unqlite_value_to_string(pKey, nullptr);
    (*result)[key] = std::move(item);
    return UNQLITE_OK;
}

int UnQLiteValue::fillJsonValueArrayCallback(unqlite_value *pKey, unqlite_value *pValue, void *pUserData) {
    Json::Value* result = reinterpret_cast<Json::Value*>(pUserData);
    Json::Value item = UnQLiteValue::convertType(pValue, UnQLiteValue::type<Json::Value>());
    result->append(std::move(item));
    return UNQLITE_OK;
}

}
