#include <mpi.h>
#include <sonata/Provider.hpp>
#include <sonata/Admin.hpp>
#include <sonata/Client.hpp>
#include <tclap/CmdLine.h>
#include <spdlog/spdlog.h>
#include <unordered_set>
#include <iostream>
#include <fstream>
#include <streambuf>

namespace tl = thallium;
namespace snt = sonata;

static std::string g_protocol        = "";
static std::string g_log_level       = "info";
static std::string g_config_file     = "";
static size_t      g_iterations      = 500;
static double      g_waittime        = 0.0;
static bool        g_progress_thread = false;
static std::string g_provider_config = "";
static std::string g_record_file     = "";

static int         g_rank        = 0;
static int         g_size        = 1;
static tl::engine  g_engine;
static std::string g_record;

static std::vector<snt::Database>   g_databases;
static std::vector<snt::Collection> g_anomalies;
static std::vector<snt::Collection> g_normalexe;
static std::vector<std::tuple<std::string, uint16_t, std::string>> g_db_names;

static std::vector<snt::Database> list_databases();
static std::vector<snt::Collection> setup_collection(const std::string& coll_name);
static std::string read_record();
static void run_client();
static void shutdown_servers();
static void parse_command_line(int argc, char **argv);

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &g_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &g_size);

    parse_command_line(argc, argv);
    spdlog::set_level(spdlog::level::from_str(g_log_level));

    auto mode = THALLIUM_CLIENT_MODE;
    if(g_config_file.empty())
        g_engine = tl::engine(g_protocol, mode, g_progress_thread);
    else {
        std::ifstream f(g_config_file.c_str());
        std::string config((std::istreambuf_iterator<char>(f)),
                            std::istreambuf_iterator<char>());
        if(config.empty()) {
            spdlog::critical("Could not read configuration file {}", g_config_file);
        }
        g_engine = tl::engine(g_protocol, mode, config.c_str());
    }

    g_record    = read_record();
    g_databases = list_databases();
    g_anomalies = setup_collection("anomalies");
    g_normalexe = setup_collection("normalexe");

    run_client();

    shutdown_servers();

    g_engine.finalize();

    MPI_Finalize();
}

void parse_command_line(int argc, char **argv) {
    try {
        TCLAP::CmdLine cmd("Sonata ProvDB test", ' ', "0.1");
        TCLAP::ValueArg<std::string> protocol("a", "address", "Mercury protocol (e.g. ofi+tcp)", true, "", "string", cmd);
        TCLAP::ValueArg<std::string> config("c", "config", "Margo configuration file", false, "", "string", cmd);
        TCLAP::ValueArg<std::string> loglevel("v", "log-level", "Log level", false, "info", "string", cmd);
        TCLAP::ValueArg<size_t> iterations("i", "iterations", "Number of iterations to complete", false, 500, "int", cmd);
        TCLAP::ValueArg<double> waittime("w", "wait-time", "Waiting time between iterations (msec)", false, 0.0, "float", cmd);
        TCLAP::SwitchArg progressThread("t", "use-progress-thread", "Use progress thread", cmd, false);
        TCLAP::ValueArg<std::string> providerConfig("p", "provider-file", "File listing provider addresses", true, "", "string", cmd);
        TCLAP::ValueArg<std::string> record("r", "record", "File containing records to insert", true, "", "string", cmd);

        cmd.parse(argc, argv);

        g_protocol        = protocol.getValue();
        g_config_file     = config.getValue();
        g_log_level       = loglevel.getValue();
        g_iterations      = iterations.getValue();
        g_waittime        = waittime.getValue();
        g_progress_thread = progressThread.getValue();
        g_provider_config = providerConfig.getValue();
        g_record_file     = record.getValue();

    } catch(TCLAP::ArgException &e) {
        std::cerr << "error: " << e.error()
                  << " for arg " << e.argId()
                  << std::endl;
        exit(-1);
    }
}

std::vector<snt::Database> list_databases() {
    std::vector<snt::Database> result;
    auto admin = snt::Admin(g_engine);
    auto client = snt::Client(g_engine);

    std::string addr;
    uint16_t provider_id;

    std::ifstream f(g_provider_config.c_str());
    while(f >> addr >> provider_id) {
        spdlog::trace("Querying address {} and provider id {} for databases", addr, provider_id);
        auto db_names = admin.listDatabases(addr, provider_id);
        for(auto& name : db_names) {
            g_db_names.emplace_back(addr, provider_id, name);
            spdlog::trace("Found database {} at address {} and provider id {}",
                         name, addr, provider_id);
            result.push_back(client.open(addr, provider_id, name));
        }
    }
    spdlog::trace("Done listing databases");

    return result;
}

std::string read_record() {
    std::ifstream t(g_record_file.c_str());
    std::string str((std::istreambuf_iterator<char>(t)),
                     std::istreambuf_iterator<char>());
    return str;
}

std::vector<snt::Collection> setup_collection(const std::string& coll_name) {
    std::vector<snt::Collection> result;
    if(g_rank == 0) {
        for(auto& db : g_databases) {
            spdlog::info("Creating collection {}", coll_name);
            result.push_back(db.create(coll_name));
        }
        MPI_Barrier(MPI_COMM_WORLD);
    } else {
        MPI_Barrier(MPI_COMM_WORLD);
        for(auto& db : g_databases) {
            result.push_back(db.open(coll_name, false));
        }
    }
    return result;
}

void run_client() {
    std::vector<std::string> records = { g_record };
    std::list<snt::AsyncRequest> requests;

    auto check_requests = [&]() {
        for(auto it = requests.begin(); it != requests.end();) {
            if(it->completed()) {
                it->wait();
                it = requests.erase(it);
            } else {
                ++it;
            }
        }
    };

    auto shard_index = g_rank % g_anomalies.size();

    for(size_t i = 0; i < g_iterations; i++) {
        MPI_Barrier(MPI_COMM_WORLD);
        check_requests();
        spdlog::info("[{}] iteration {}, {} pending requests", g_rank, i, requests.size());
        snt::AsyncRequest req1, req2;
        g_anomalies[shard_index].store_multi(records, nullptr, false, &req1);
        g_normalexe[shard_index].store_multi(records, nullptr, false, &req2);
        requests.push_back(std::move(req1));
        requests.push_back(std::move(req2));
        tl::thread::sleep(g_engine, g_waittime);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    spdlog::info("[{}] loop completed, waiting for {} remaining requests...", g_rank, requests.size());
    auto it = requests.begin();
    while(!requests.empty()) {
        it->wait();
        it = requests.erase(it);
    }
    spdlog::info("[{}] done!", g_rank);
    MPI_Barrier(MPI_COMM_WORLD);
}

static void destroy_databases() {
    auto admin = snt::Admin(g_engine);

    for(auto& t : g_db_names) {
        auto& addr = std::get<0>(t);
        auto provider_id = std::get<1>(t);
        auto& name = std::get<2>(t);
        admin.destroyDatabase(addr, provider_id, name);
    }
}

void shutdown_servers() {
    if(g_rank != 0)
        return;

    destroy_databases();
    std::unordered_set<std::string> already_shut_down;
    for(auto& t : g_db_names) {
        auto& addr = std::get<0>(t);
        if(already_shut_down.count(addr) != 0)
            continue;
        already_shut_down.insert(addr);
        g_engine.shutdown_remote_engine(g_engine.lookup(addr));
    }
}
