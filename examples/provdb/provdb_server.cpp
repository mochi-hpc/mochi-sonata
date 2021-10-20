#include <mpi.h>
#include <sonata/Provider.hpp>
#include <sonata/Admin.hpp>
#include <sonata/Client.hpp>
#include <tclap/CmdLine.h>
#include <spdlog/spdlog.h>
#include <iostream>
#include <fstream>
#include <streambuf>

namespace tl = thallium;
namespace snt = sonata;

static std::string              g_protocol        = "";
static std::string              g_log_level       = "info";
static std::string              g_config_file     = "";
static std::string              g_provider_config = "";
static std::string              g_output_file     = "";
static bool                     g_progress_thread = false;
static std::vector<std::string> g_providers_desc;

static int                        g_rank        = 0;
static int                        g_size        = 1;
static tl::engine                 g_engine;
static std::vector<snt::Provider> g_providers;
static std::vector<uint16_t>      g_provider_ids;

static void setup_server();
static void cleanup();
static void parse_command_line(int argc, char **argv);

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &g_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &g_size);

    parse_command_line(argc, argv);
    spdlog::set_level(spdlog::level::from_str(g_log_level));

    if(g_config_file.empty())
        g_engine = tl::engine(g_protocol, THALLIUM_SERVER_MODE);
    else {
        std::ifstream f(g_config_file.c_str());
        std::string config((std::istreambuf_iterator<char>(f)),
                            std::istreambuf_iterator<char>());
        if(config.empty()) {
            spdlog::critical("Could not read configuration file {}", g_config_file);
        }
        g_engine = tl::engine(g_protocol, THALLIUM_SERVER_MODE, config);
    }
    g_engine.enable_remote_shutdown();

    setup_server();

    g_engine.push_finalize_callback(cleanup);

    spdlog::info("[{}] Server running at address {}", g_rank, static_cast<std::string>(g_engine.self()));
    g_engine.wait_for_finalize();
    spdlog::info("[{}] Server shutting down", g_rank);

    MPI_Finalize();
}

void parse_command_line(int argc, char **argv) {
    try {
        TCLAP::CmdLine cmd("Sonata ProvDB test", ' ', "0.1");
        TCLAP::ValueArg<std::string> protocol("a", "address", "Mercury protocol (e.g. ofi+tcp)", true, "", "string", cmd);
        TCLAP::ValueArg<std::string> config("c", "config", "Server configuration file", false, "", "string", cmd);
        TCLAP::ValueArg<std::string> loglevel("v", "log-level", "Log level", false, "info", "string", cmd);
        TCLAP::MultiArg<std::string> providers("p", "provider", "Provider description (e.g. \"42:pool_a\")", true, "string", cmd);
        TCLAP::ValueArg<std::string> providerConfig("s", "sonata-config", "Sonata configuration file for providers", false, "", "string", cmd);
        TCLAP::ValueArg<std::string> outFile("o", "output-file", "File containing server information", false, "", "string", cmd);

        cmd.parse(argc, argv);

        g_protocol        = protocol.getValue();
        g_config_file     = config.getValue();
        g_log_level       = loglevel.getValue();
        g_providers_desc  = providers.getValue();
        g_provider_config = providerConfig.getValue();
        g_output_file     = outFile.getValue();

    } catch(TCLAP::ArgException &e) {
        std::cerr << "error: " << e.error()
                  << " for arg " << e.argId()
                  << std::endl;
        exit(-1);
    }
}

static std::pair<uint16_t, std::string> parse_provider_description(const std::string& desc) {
    auto p = desc.find(':');
    if(p == std::string::npos) {
        return std::make_pair<uint16_t, std::string>(atoi(desc.c_str()), "");
    } else {
        return std::make_pair<uint16_t, std::string>(
                atoi(desc.substr(0, p).c_str()), desc.substr(p+1));
    }
}

static std::string create_config_for_provider(const std::string& base_config, uint16_t id) {
    auto config = base_config;
    auto p = config.find("${ID}");
    while(p != std::string::npos) {
        config.replace(p, 5, std::to_string(id));
        p = config.find("${ID}");
    };
    p = config.find("${RANK}");
    while(p != std::string::npos) {
        config.replace(p, 7, std::to_string(g_rank));
        p = config.find("${RANK}");
    };
    return config;
}

void setup_server() {
    std::unordered_map<uint16_t, std::string> provider_info;
    auto mid = g_engine.get_margo_instance();
    for(auto& desc : g_providers_desc) {
        auto p = parse_provider_description(desc);
        if(provider_info.count(p.first) != 0) {
            spdlog::critical("Found multiple providers with the same id ({})", p.first);
            exit(-1);
        } else if((!p.second.empty()) && (margo_get_pool_index(mid, p.second.c_str()) == -1)) {
            spdlog::critical("Could not find pool {} for provider {}", p.second, p.first);
            exit(-1);
        }
        provider_info[p.first] = p.second;
    }

    std::string base_config = "";
    if(!g_provider_config.empty()) {
        std::ifstream f(g_provider_config.c_str());
        std::string config((std::istreambuf_iterator<char>(f)),
                            std::istreambuf_iterator<char>());
        if(config.empty()) {
            spdlog::critical("Could not read configuration file {}", g_config_file);
            exit(-1);
        }
        base_config = std::move(config);
    }
    g_providers.reserve(provider_info.size());
    for(const auto& p : provider_info) {
        ABT_pool pool = ABT_POOL_NULL;
        if(0 != margo_get_pool_by_name(mid, p.second.c_str(), &pool)) {
            margo_get_pool_by_index(mid, 0, &pool);
        }
        auto config = create_config_for_provider(base_config, p.first);
        g_providers.emplace_back(g_engine, p.first, config, tl::pool(pool));
        g_provider_ids.push_back(p.first);
    }

    auto addr = static_cast<std::string>(g_engine.self());
    addr.resize(256,'\0');
    std::vector<char> all_addresses(256*g_size);
    MPI_Gather(addr.c_str(), 256, MPI_BYTE, all_addresses.data(), 256, MPI_BYTE, 0, MPI_COMM_WORLD);
    if(g_rank == 0 && !g_output_file.empty()) {
        std::ofstream out(g_output_file.c_str(), std::ios_base::out|std::ios_base::app);
        for(unsigned i=0; i < g_size; i++) {
        for(auto& p : provider_info) {
            auto provider_id = p.first;
            out << (all_addresses.data()+i*256) << " " << provider_id << std::endl;
        }
        }
    }
}

void cleanup() {
    if(g_rank == 0)
        std::remove(g_output_file.c_str());
}
