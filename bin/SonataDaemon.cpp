/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <iostream>
#include <sonata/Provider.hpp>
#include <spdlog/spdlog.h>
#include <tclap/CmdLine.h>
#include <vector>
#include <fstream>
#include <streambuf>

namespace tl = thallium;
namespace snt = sonata;

static std::string g_address = "na+sm";
static unsigned g_num_providers = 1;
static int g_num_threads = 0;
static std::string g_log_level = "info";
static bool g_use_progress_thread = false;
static std::string g_config = "";

static void parse_command_line(int argc, char **argv);

int main(int argc, char **argv) {
  parse_command_line(argc, argv);
  tl::engine engine;
  if(g_config.empty()) {
    engine = tl::engine(g_address, THALLIUM_SERVER_MODE,
                        g_use_progress_thread, g_num_threads);
  } else {
    std::ifstream f(g_config.c_str());
    std::string config((std::istreambuf_iterator<char>(f)),
                        std::istreambuf_iterator<char>());
    engine = tl::engine(g_address, THALLIUM_SERVER_MODE,
                        config.c_str());
  }
  engine.enable_remote_shutdown();
  std::vector<snt::Provider> providers;
  for (unsigned i = 0; i < g_num_providers; i++) {
    providers.emplace_back(engine, i);
  }
  spdlog::info("Server running at address {}", (std::string)engine.self());
  engine.wait_for_finalize();
  return 0;
}

void parse_command_line(int argc, char **argv) {
  try {
    TCLAP::CmdLine cmd("Spawns a Sonata daemon", ' ', "0.1");
    TCLAP::ValueArg<std::string> addressArg(
        "a", "address", "Address or protocol (e.g. ofi+tcp)", true, "",
        "string");
    TCLAP::ValueArg<unsigned> providersArg(
        "n", "num-providers", "Number of providers to spawn (default 1)", false,
        1, "int");
    TCLAP::SwitchArg progressThreadArg("p", "use-progress-thread",
                                       "Use a Mercury progress thread", cmd,
                                       false);
    TCLAP::ValueArg<int> numThreads("t", "num-threads",
                                    "Number of threads for RPC handlers", false,
                                    0, "int");
    TCLAP::ValueArg<std::string> configArg("c", "config",
                                           "Thallium engine JSON configuration file", false,
                                           "", "string");
    TCLAP::ValueArg<std::string> logLevel(
        "v", "verbose",
        "Log level (trace, debug, info, warning, error, critical, off)", false,
        "info", "string");
    cmd.add(addressArg);
    cmd.add(providersArg);
    cmd.add(numThreads);
    cmd.add(logLevel);
    cmd.add(configArg);
    cmd.parse(argc, argv);
    g_log_level = logLevel.getValue();
    spdlog::set_level(spdlog::level::from_str(g_log_level));
    g_address = addressArg.getValue();
    g_num_providers = providersArg.getValue();
    g_num_threads = numThreads.getValue();
    g_use_progress_thread = progressThreadArg.getValue();
    g_config = configArg.getValue();
    if(configArg.isSet() && numThreads.isSet())
        spdlog::warn("--config and --num-threads both set, --num-threads will be ignored");
    if(configArg.isSet() && progressThreadArg.isSet())
        spdlog::warn("--config and --use-progress-thread both set, --use-progress-thread will be ignored");
  } catch (TCLAP::ArgException &e) {
    std::cerr << "error: " << e.error() << " for arg " << e.argId()
              << std::endl;
    exit(-1);
  }
}
