/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include <functional>
#include "UnQLiteVM.hpp"
#include "UnQLiteBackend.hpp"
#include "sonata/AsyncRequest.hpp"

namespace sonata {

typedef std::function<void(unqlite_context*, bool)> RequestCompletionFn;

#define LOG_ERROR()\
    unqlite_context_throw_error_format(pCtx, UNQLITE_CTX_WARNING,\
        "%s threw an exception: %s", unqlite_result_null(pCtx), ex.what());

#define CHECK_ARGS(__num__) do {\
    if(argc != __num__) {\
        unqlite_context_throw_error_format(pCtx, UNQLITE_CTX_ERR,\
            "%s: unexpected number of aruments (%d, expected %d)",\
           unqlite_function_name(pCtx), argc, __num__);\
        unqlite_result_null(pCtx);\
        return UNQLITE_OK;\
    }} while(false)

#define CHECK_ARGS2(__num1__, __num2__) do {\
    if(argc != (__num1__) && argc != (__num2__)) {\
        unqlite_context_throw_error_format(pCtx, UNQLITE_CTX_ERR,\
            "%s: unexpected number of aruments (%d, expected %d or %d)",\
            unqlite_function_name(pCtx), argc, (__num1__), (__num2__));\
        unqlite_result_null(pCtx);\
        return UNQLITE_OK;\
    }} while(false)

#define CATCH_AND_ABORT()\
    catch(const Exception& ex) {\
        unqlite_context_throw_error_format(pCtx, UNQLITE_CTX_ERR,\
            "%s threw an exception: %s", unqlite_function_name(pCtx), ex.what());\
        unqlite_result_null(pCtx);\
        return UNQLITE_OK;\
    }

template<typename ResultType>
void test_or_wait_completion(unqlite_context* pCtx, bool wait, AsyncRequest* req, ResultType* result) {
    // TODO
    if(wait) {
        delete result;
    } else {

    }
}

struct database_info {

    std::string address;
    uint16_t    provider_id;
    std::string db_name;

    database_info() = default;

    database_info(unqlite_value* v) {
        UnQLiteValue x(v, nullptr, nullptr, true);
        address = x["address"].as<std::string>();
        provider_id = x["provider_id"].as<uint16_t>();
        db_name = x["database_name"].as<std::string>();
    }

    database_info(const database_info&) = default;
    database_info& operator=(const database_info&) = default;
    database_info(database_info&&) = default;
    database_info& operator=(database_info&&) = default;
    ~database_info() = default;
};

struct collection_info {
    
    database_info db_info;
    std::string coll_name;

    collection_info() = default;

    collection_info(unqlite_value* v) {
        UnQLiteValue x(v, nullptr, nullptr, true);
        coll_name = x["collection_name"].as<std::string>();
        unqlite_value* db_info_u = unqlite_array_fetch(v, "database", -1);
        if(!db_info_u) {
            throw Exception("Could not find \"database\" entry in collection information");
        }
        db_info = database_info(db_info_u);
    }

    collection_info(const collection_info&) = default;
    collection_info& operator=(const collection_info&) = default;
    collection_info(collection_info&&) = default;
    collection_info& operator=(collection_info&&) = default;
    ~collection_info() = default;
};

void UnQLiteVM::get_script(unqlite_value *pValue, void *pUserData) {
    auto vm = reinterpret_cast<UnQLiteVM*>(pUserData);
    unqlite_value_string(pValue, vm->m_code, -1);
}

int UnQLiteVM::snta_db_create(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS2(5, 6);
    // get parameters
    std::string address(unqlite_value_to_string(argv[0], nullptr));
    uint16_t provider_id = unqlite_value_to_int(argv[1]);
    std::string db_name(unqlite_value_to_string(argv[2], nullptr));
    std::string db_type(unqlite_value_to_string(argv[3], nullptr));
    std::stringstream config;
    UnQLiteValue config_param(argv[4], nullptr, nullptr, true);
    config_param.printToStream(config);
    std::string token = (argc == 6) ? unqlite_value_to_string(argv[5], nullptr) : "";
    // call Admin function
    try {
        vm->m_backend->m_admin.createDatabase(
                address,
                provider_id,
                db_name,
                db_type,
                config.str(),
                token);
        unqlite_result_bool(pCtx, true);
    } CATCH_AND_ABORT();
    return UNQLITE_OK;
}

int UnQLiteVM::snta_db_attach(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS2(5, 6);
    // get parameters
    std::string address(unqlite_value_to_string(argv[0], nullptr));
    uint16_t provider_id = unqlite_value_to_int(argv[1]);
    std::string db_name(unqlite_value_to_string(argv[2], nullptr));
    std::string db_type(unqlite_value_to_string(argv[3], nullptr));
    std::stringstream config;
    UnQLiteValue config_param(argv[4], nullptr, nullptr, true);
    config_param.printToStream(config);
    std::string token = (argc == 6) ? unqlite_value_to_string(argv[5], nullptr) : "";
    // call Admin function
    try {
        vm->m_backend->m_admin.attachDatabase(
                address,
                provider_id,
                db_name,
                db_type,
                config.str(),
                token);
        unqlite_result_bool(pCtx, true);
    } CATCH_AND_ABORT();
    return UNQLITE_OK;
}

int UnQLiteVM::snta_db_detach(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS2(3, 4);
    // get parameters
    std::string address(unqlite_value_to_string(argv[0], nullptr));
    uint16_t provider_id = unqlite_value_to_int(argv[1]);
    std::string db_name(unqlite_value_to_string(argv[2], nullptr));
    std::string token = (argc == 4) ? unqlite_value_to_string(argv[3], nullptr) : "";
    // call Admin function
    try {
        vm->m_backend->m_admin.detachDatabase(
                address,
                provider_id,
                db_name,
                token);
        unqlite_result_bool(pCtx, true);
    } CATCH_AND_ABORT();
    return UNQLITE_OK;
}

int UnQLiteVM::snta_db_destroy(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS2(3, 4);
    // get parameters
    std::string address(unqlite_value_to_string(argv[0], nullptr));
    uint16_t provider_id = unqlite_value_to_int(argv[1]);
    std::string db_name(unqlite_value_to_string(argv[2], nullptr));
    std::string token = (argc == 4) ? unqlite_value_to_string(argv[3], nullptr) : "";
    // call Admin function
    try {
        vm->m_backend->m_admin.destroyDatabase(
                address,
                provider_id,
                db_name,
                token);
        unqlite_result_bool(pCtx, true);
    } CATCH_AND_ABORT();
    return UNQLITE_OK;
}

int UnQLiteVM::sntd_coll_create(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS(2);
    try {
        database_info db_info(argv[0]);
        std::string coll_name = unqlite_value_to_string(argv[1], nullptr);
        if(coll_name.size() == 0) {
            throw Exception("Invalid collection name argument");
        }
        Database db = vm->m_backend->m_client.open(db_info.address, db_info.provider_id, db_info.db_name, false);
        bool b = true;
        try {
            db.create(coll_name);
        } catch(...) {
            b = false;
        }
        int ret = unqlite_result_bool(pCtx, b);
        if(ret != UNQLITE_OK)
            throw Exception("Could not set result in unqlite context");
    } CATCH_AND_ABORT();
    return UNQLITE_OK;
}

int UnQLiteVM::sntd_coll_exists(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS(2);
    try {
        database_info db_info(argv[0]);
        std::string coll_name = unqlite_value_to_string(argv[1], nullptr);
        if(coll_name.size() == 0)
            throw Exception("Invalid collection name argument");
        Database db = vm->m_backend->m_client.open(db_info.address, db_info.provider_id, db_info.db_name, false);
        bool b = db.exists(coll_name);
        int ret = unqlite_result_bool(pCtx, b);
        if(ret != UNQLITE_OK)
            throw Exception("Could not set result in unqlite context");
    } CATCH_AND_ABORT();
    return UNQLITE_OK;
}

int UnQLiteVM::sntd_coll_open(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS(2);
    try {
        database_info db_info(argv[0]);
        std::string coll_name = unqlite_value_to_string(argv[1], nullptr);
        if(coll_name.size() == 0)
            throw Exception("Invalid collection name argument");
        Database db = vm->m_backend->m_client.open(db_info.address, db_info.provider_id, db_info.db_name, false);
        bool b = db.exists(coll_name);
        unqlite_value* result;
        int ret;
        if(b) {
            result = unqlite_context_new_array(pCtx);
            unqlite_array_add_strkey_elem(result, "database_info", argv[0]);
            unqlite_array_add_strkey_elem(result, "collection_name", argv[1]);
            ret = unqlite_result_value(pCtx, result);
        } else {
            result = unqlite_context_new_scalar(pCtx);
            unqlite_value_null(result);
            ret = unqlite_result_value(pCtx, result);
        }
        if(ret != UNQLITE_OK)
            throw Exception("Could not set result in unqlite context");
    } CATCH_AND_ABORT();
    return UNQLITE_OK;
}

int UnQLiteVM::sntd_coll_drop(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS(2);
    try {
        database_info db_info(argv[0]);
        std::string coll_name = unqlite_value_to_string(argv[1], nullptr);
        if(coll_name.size() == 0) {
            throw Exception("Invalid collection name argument");
        }
        Database db = vm->m_backend->m_client.open(db_info.address, db_info.provider_id, db_info.db_name, false);
        bool b = true;
        try {
            db.drop(coll_name);
        } catch(...) {
            b = false;
        }
        int ret = unqlite_result_bool(pCtx, b);
        if(ret != UNQLITE_OK)
            throw Exception("Could not set result in unqlite context");

    } CATCH_AND_ABORT();
    return UNQLITE_OK;
}

int UnQLiteVM::sntd_execute(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS2(2, 3);
    try {
        database_info db_info(argv[0]);
        Database db = vm->m_backend->m_client.open(db_info.address, db_info.provider_id, db_info.db_name, false);
        std::string code;
        if(unqlite_value_is_callable(argv[1])) {
            std::string function_name = unqlite_value_to_string(argv[1], nullptr);
            code = vm->extract_function_code(function_name.c_str());
            if(code.empty()) {
                throw Exception("Could not find source code for function "s+function_name);
            }
            code += "\n"s + function_name + "();\n";
        } else if(unqlite_value_is_string(argv[1])) {
            code = unqlite_value_to_string(argv[1], nullptr);
        } else {
            throw Exception("Invalid 2nd argument type (expected function or string)");
        }
        std::unordered_set<std::string> vars;
        if(argc == 3) {
            std::vector<std::string> variables = UnQLiteValue(argv[2], nullptr, nullptr, true);
            for(auto& v : variables) { 
                vars.insert(v);
            }
        }
        std::unordered_map<std::string,std::string> result;
        db.execute(code, vars, &result);
        unqlite_value* result_dict = unqlite_context_new_scalar(pCtx);
        for(auto it = result.begin(); it != result.end(); it++) {
            unqlite_value* v = unqlite_context_new_scalar(pCtx);
            unqlite_value_string(v, it->second.c_str(), -1);
            unqlite_array_add_strkey_elem(result_dict, it->first.c_str(), v);
        }
        unqlite_result_value(pCtx, result_dict);
    } CATCH_AND_ABORT();
    return UNQLITE_OK;
}

int UnQLiteVM::sntc_store(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS2(2, 3);
    try {
        collection_info coll_info(argv[0]);
        database_info& db_info = coll_info.db_info;
        Database db = vm->m_backend->m_client.open(db_info.address, db_info.provider_id, db_info.db_name, false);
        Collection coll = db.open(coll_info.coll_name, false);
    
        if(unqlite_value_is_empty(argv[1])
        || unqlite_value_is_resource(argv[1])
        || unqlite_value_is_callable(argv[1])) {
            throw Exception("Unsupported record type");
        }

        const char* document = unqlite_value_to_string(argv[1], nullptr);
        uint64_t id = coll.store(document);

        unqlite_result_int64(pCtx, id);
    } CATCH_AND_ABORT();
    return UNQLITE_OK;
}

int UnQLiteVM::sntc_fetch(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS2(2, 3);
    try {
        collection_info coll_info(argv[0]);
        database_info& db_info = coll_info.db_info;
        Database db = vm->m_backend->m_client.open(db_info.address, db_info.provider_id, db_info.db_name, false);
        Collection coll = db.open(coll_info.coll_name, false);
        
        if(!unqlite_value_is_int(argv[1])) {
            throw Exception("Invalid argument type, expected integer");
        }
        uint64_t id = unqlite_value_to_int64(argv[1]);

        Json::Value result;
        coll.fetch(id, &result);
        UnQLiteValue uql_result(result, pCtx);
        unqlite_result_value(pCtx, uql_result.m_value);
    } CATCH_AND_ABORT();
    return UNQLITE_OK;
}

int UnQLiteVM::sntc_filter(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS2(2, 3);
    try {
        collection_info coll_info(argv[0]);
        database_info& db_info = coll_info.db_info;
        Database db = vm->m_backend->m_client.open(db_info.address, db_info.provider_id, db_info.db_name, false);
        Collection coll = db.open(coll_info.coll_name, false);
        
        std::string code;
        if(unqlite_value_is_callable(argv[1])) {
            std::string function_name = unqlite_value_to_string(argv[1], nullptr);
            code = vm->extract_function_code(function_name.c_str());
            if(code.empty()) {
                throw Exception("Could not find source code for function "s+function_name);
            }
        } else if(unqlite_value_is_string(argv[1])) {
            code = unqlite_value_to_string(argv[1], nullptr);
        } else {
            throw Exception("Invalid 2nd argument type (expected function or string)");
        }

        Json::Value result;
        coll.filter(code, &result);

        UnQLiteValue uql_result(result, pCtx);
        unqlite_result_value(pCtx, uql_result.m_value);

    } CATCH_AND_ABORT();
    return UNQLITE_OK;
}

int UnQLiteVM::sntc_update(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS2(3, 4);
    try {
        collection_info coll_info(argv[0]);
        database_info& db_info = coll_info.db_info;
        Database db = vm->m_backend->m_client.open(db_info.address, db_info.provider_id, db_info.db_name, false);
        Collection coll = db.open(coll_info.coll_name, false);
   
        if(!unqlite_value_is_int(argv[1])) {
            throw Exception("Invalid argument type, expected integer");
        }
        uint64_t id = unqlite_value_to_int64(argv[1]);
        
        if(unqlite_value_is_empty(argv[2])
        || unqlite_value_is_resource(argv[2])
        || unqlite_value_is_callable(argv[2])) {
            throw Exception("Unsupported record type");
        }
        const char* new_document = unqlite_value_to_string(argv[2], nullptr);
        coll.update(id, new_document);
        unqlite_result_bool(pCtx, true);

    } CATCH_AND_ABORT();
    return UNQLITE_OK;

}

int UnQLiteVM::sntc_all(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS2(1, 2);
    try {
        collection_info coll_info(argv[0]);
        database_info& db_info = coll_info.db_info;
        Database db = vm->m_backend->m_client.open(db_info.address, db_info.provider_id, db_info.db_name, false);
        Collection coll = db.open(coll_info.coll_name, false);
        
        Json::Value result;
        coll.all(&result);
        UnQLiteValue uql_result(result, pCtx);
        unqlite_result_value(pCtx, uql_result.m_value);
    } CATCH_AND_ABORT();
    return UNQLITE_OK;
}

int UnQLiteVM::sntc_last_record_id(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS(1);
    try {
        collection_info coll_info(argv[0]);
        database_info& db_info = coll_info.db_info;
        Database db = vm->m_backend->m_client.open(db_info.address, db_info.provider_id, db_info.db_name, false);
        Collection coll = db.open(coll_info.coll_name, false);
        uint64_t last_record = coll.last_record_id();
        unqlite_result_int64(pCtx, last_record);
    } CATCH_AND_ABORT();
    return UNQLITE_OK;
}

int UnQLiteVM::sntc_size(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS(1);
    try {
        collection_info coll_info(argv[0]);
        database_info& db_info = coll_info.db_info;
        Database db = vm->m_backend->m_client.open(db_info.address, db_info.provider_id, db_info.db_name, false);
        Collection coll = db.open(coll_info.coll_name, false);
        size_t size = coll.size();
        unqlite_result_int64(pCtx, size);
    } CATCH_AND_ABORT();
    return UNQLITE_OK;
}

int UnQLiteVM::sntc_erase(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS2(2,3);
    try {
        collection_info coll_info(argv[0]);
        database_info& db_info = coll_info.db_info;
        Database db = vm->m_backend->m_client.open(db_info.address, db_info.provider_id, db_info.db_name, false);
        Collection coll = db.open(coll_info.coll_name, false);

        if(!unqlite_value_is_int(argv[1])) {
            throw Exception("Invalid argument type, expected integer");
        }
        uint64_t id = unqlite_value_to_int64(argv[1]);

        Json::Value result;
        coll.erase(id);
        unqlite_result_bool(pCtx, true);

    } CATCH_AND_ABORT();
    return UNQLITE_OK;
}

int UnQLiteVM::sntr_wait(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS(1);
    try {
        if(!unqlite_value_is_resource(argv[0])) {
            throw Exception("Invalid argument (not an asynchronous request object)");
        }
        void* ptr = unqlite_value_to_resource(argv[0]);
        auto completion_fn = reinterpret_cast<RequestCompletionFn*>(ptr);
        (*completion_fn)(pCtx, true);
        delete completion_fn; // XXX we should use placement new/delete to allocate in the context
        unqlite_context_release_value(pCtx, argv[0]);
    } CATCH_AND_ABORT();
    return UNQLITE_OK;
}

int UnQLiteVM::sntr_test(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS(1);
    try {
        if(!unqlite_value_is_resource(argv[0])) {
            throw Exception("Invalid argument (not an asynchronous request object)");
        }
        void* ptr = unqlite_value_to_resource(argv[0]);
        auto completion_fn = reinterpret_cast<RequestCompletionFn*>(ptr);
        (*completion_fn)(pCtx, false);
    } CATCH_AND_ABORT();
    return UNQLITE_OK;
}

}
