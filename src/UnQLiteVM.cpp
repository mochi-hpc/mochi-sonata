/*
 * (C) 2020 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include "UnQLiteVM.hpp"
#include "UnQLiteBackend.hpp"

namespace sonata {

#define CHECK_ARGS(__fun__, __num__) do {\
    if(argc != __num__) {\
        unqlite_context_throw_error_format(pCtx, UNQLITE_CTX_ERR,\
            #__fun__ ": unexpected number of aruments (%d, expected %d)", argc, __num__);\
        unqlite_result_null(pCtx);\
        return UNQLITE_OK;\
    }} while(false)

#define CHECK_ARGS2(__fun__, __num1__, __num2__) do {\
    if(argc != (__num1__) && argc != (__num2__)) {\
        unqlite_context_throw_error_format(pCtx, UNQLITE_CTX_ERR,\
            #__fun__ ": unexpected number of aruments (%d, expected %d or %d)", argc, (__num1__), (__num2__));\
        unqlite_result_null(pCtx);\
        return UNQLITE_OK;\
    }} while(false)

#define CATCH_AND_ABORT(__fun__)\
    catch(const Exception& ex) {\
        unqlite_context_throw_error_format(pCtx, UNQLITE_CTX_ERR,\
            #__fun__ " threw an exception: %s", ex.what());\
        unqlite_result_null(pCtx);\
        return UNQLITE_OK;\
    }

#define LOG_ERROR(__fun__)\
    unqlite_context_throw_error_format(pCtx, UNQLITE_CTX_WARNING,\
            #__fun__ " threw an exception: %s", ex.what());

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
    CHECK_ARGS2(snta_db_create, 5, 6);
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
    } CATCH_AND_ABORT(snta_db_create);
    return UNQLITE_OK;
}

int UnQLiteVM::snta_db_attach(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS2(snta_db_attach, 5, 6);
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
    } CATCH_AND_ABORT(snta_db_attach);
    return UNQLITE_OK;
}

int UnQLiteVM::snta_db_detach(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS2(snta_db_detach, 3, 4);
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
    } CATCH_AND_ABORT(snta_db_detach);
    return UNQLITE_OK;
}

int UnQLiteVM::snta_db_destroy(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS2(snta_db_destroy, 3, 4);
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
    } CATCH_AND_ABORT(snta_db_destroy);
    return UNQLITE_OK;
}

int UnQLiteVM::sntd_coll_create(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS(sntd_coll_create, 2);
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
    } CATCH_AND_ABORT(sntd_coll_create);
    return UNQLITE_OK;
}

int UnQLiteVM::sntd_coll_exists(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS(sntd_coll_exists, 2);
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
    } CATCH_AND_ABORT(sntd_coll_exists);
    return UNQLITE_OK;
}

int UnQLiteVM::sntd_coll_open(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS(sntd_coll_exists, 2);
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
    } CATCH_AND_ABORT(sntd_coll_exists);
    return UNQLITE_OK;
}

int UnQLiteVM::sntd_coll_drop(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    CHECK_ARGS(sntd_coll_drop, 2);
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

    } CATCH_AND_ABORT(sntd_coll_drop);
    return UNQLITE_OK;
}

int UnQLiteVM::sntd_execute(unqlite_context *pCtx, int argc, unqlite_value **argv) {

    // TODO
    return UNQLITE_OK;
}

int UnQLiteVM::sntc_store(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    UnQLiteVM* vm = reinterpret_cast<UnQLiteVM*>(unqlite_context_user_data(pCtx));
    // TODO
    return UNQLITE_OK;
}

int UnQLiteVM::sntc_fetch(unqlite_context *pCtx, int argc, unqlite_value **argv) {

    // TODO
    return UNQLITE_OK;
}

int UnQLiteVM::sntc_filter(unqlite_context *pCtx, int argc, unqlite_value **argv) {

    // TODO
    return UNQLITE_OK;
}

int UnQLiteVM::sntc_update(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    // TODO
    return UNQLITE_OK;

}

int UnQLiteVM::sntc_all(unqlite_context *pCtx, int argc, unqlite_value **argv) {

    // TODO
    return UNQLITE_OK;
}

int UnQLiteVM::sntc_last_record_id(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    // TODO
    return UNQLITE_OK;

}

int UnQLiteVM::sntc_size(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    // TODO
    return UNQLITE_OK;

}

int UnQLiteVM::sntc_erase(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    // TODO
    return UNQLITE_OK;

}

int UnQLiteVM::sntr_wait(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    // TODO
    return UNQLITE_OK;

}

int UnQLiteVM::sntr_test(unqlite_context *pCtx, int argc, unqlite_value **argv) {
    // TODO
    return UNQLITE_OK;

}

}
