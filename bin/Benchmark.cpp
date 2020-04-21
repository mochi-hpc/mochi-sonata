#include <random>
#include <iostream>
#include <iomanip>
#include <cmath>
#include <algorithm>
#include <numeric>
#include <fstream>
#include <map>
#include <functional>
#include <memory>
#include <mpi.h>
#include <sonata/Provider.hpp>
#include <sonata/Client.hpp>
#include <sonata/Admin.hpp>

namespace tl = thallium;
namespace snt = sonata;

using namespace std::string_literals;

static std::string gen_random_string(size_t len) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    std::string s(len, ' ');
    for (unsigned i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
    return s;
}

struct RecordInfo {

    size_t num = 0;
    size_t fields = 0;
    size_t min_key_size = 0;
    size_t max_key_size = 0;
    size_t min_val_size = 0;
    size_t max_val_size = 0;
    std::uniform_real_distribution<double> unif = std::uniform_real_distribution<double>(0,1);
    std::default_random_engine rng;

    RecordInfo(const Json::Value& config) {
        num = config.get("num", 0).asUInt64();
        fields = config.get("fields", 0).asUInt64();
        if(config["key-size"]) {
            if(config["key-size"].isArray() && config["key-size"].size() == 2) {
                min_key_size = config["key-size"][0].asUInt64();
                max_key_size = config["key-size"][1].asUInt64();
            } else if(config["key-size"].isIntegral()) {
                min_key_size = config["key-size"].asUInt64();
                max_key_size = min_key_size;
            } else {
                throw std::runtime_error("invalid key-size field");
            }
        }
        if(min_key_size == 0 || min_key_size > max_key_size) {
            throw std::runtime_error("invalid key-size value(s)");
        }
        if(config["val-size"]) {
            if(config["val-size"].isArray() && config["val-size"].size() == 2) {
                min_val_size = config["val-size"][0].asUInt64();
                max_val_size = config["val-size"][1].asUInt64();
            } else if(config["val-size"].isIntegral()) {
                min_val_size = config["val-size"].asUInt64();
                max_val_size = min_key_size;
            } else {
                throw std::runtime_error("invalid val-size field");
            }
            if(min_val_size == 0 || min_val_size > max_val_size) {
                throw std::runtime_error("invalid val-size value(s)");
            }
        }
    }

    void generateRandomRecordString(std::string* result) {
        std::stringstream ss;
        ss << "{ ";
        for(size_t i = 0; i < fields; i++) {
            size_t ks = min_key_size + (rand() % (max_key_size - min_key_size + 1));
            std::string key = gen_random_string(ks);
            size_t vs = min_val_size + (rand() % (max_val_size - min_val_size + 1));
            std::string val = gen_random_string(vs);
            ss << "\"" << key << "\" : \"" << val << "\"";
            ss << ", ";
        }

        ss << "\"__p__\" : " << std::setprecision(12) << unif(rng);
        ss << " }";
        *result = ss.str();
    }

    void generateRandomRecordString(Json::Value* result) {
        Json::Value record;
        for(size_t i = 0; i < fields; i++) {
            size_t ks = min_key_size + (rand() % (max_key_size - min_key_size + 1));
            std::string key = gen_random_string(ks);
            size_t vs = min_val_size + (rand() % (max_val_size - min_val_size + 1));
            std::string val = gen_random_string(vs);
            record[key] = val;
        }
        *result = record;
    }
};

struct CollectionInfo {

    std::string type;
    std::string path;
    std::string database_name;
    std::string collection_name;
    
    CollectionInfo(const Json::Value& config) {
        type = config.get("type", "unqlite").asString();
        path = config.get("path", ".").asString();
        database_name = config.get("database-name", "").asString();
        collection_name = config.get("collection-name", "").asString();
        if(database_name.size() == 0) {
            throw std::runtime_error("invalid database name");
        }
        if(collection_name.size() == 0) {
            throw std::runtime_error("invalid collection name");
        }
    }

    snt::Collection createDatabaseAndCollection(snt::Client& client, snt::Admin& admin, const std::string& address) {
        std::string db_config = "{ \"path\" : \""s + path + "\" }";
        admin.createDatabase(address, 0, database_name, type, db_config);
        snt::Database db = client.open(address, 0, database_name);
        return db.create(collection_name);
    }

    void eraseDatabaseAndCollection(snt::Client& client, snt::Admin& admin, const std::string& address) {
        admin.destroyDatabase(address, 0, database_name);
    }
};

template<typename T>
class BenchmarkRegistration;

/**
 * The AbstractBenchmark class describes an interface that a benchmark object
 * needs to satisfy. This interface has a setup, execute, and teardown
 * methods. AbstractBenchmark also acts as a factory to create concrete instances.
 */
class AbstractBenchmark {

    MPI_Comm    m_comm;   // communicator gathering all clients
    std::string m_server_addr; // server address
    snt::Client m_client; // Sonata client
    snt::Admin  m_admin;  // Sonata admin

    template<typename T>
    friend class BenchmarkRegistration;

    using benchmark_factory_function = std::function<
        std::unique_ptr<AbstractBenchmark>(Json::Value&, MPI_Comm, 
                const std::string& addr, const snt::Client&, const snt::Admin&)>;
    static std::map<std::string, benchmark_factory_function> s_benchmark_factories;

    protected:

    snt::Client& client() { return m_client; }
    snt::Admin& admin() { return m_admin; }
    MPI_Comm comm() const { return m_comm; }
    const std::string& server_addr() const { return m_server_addr; }

    public:

    AbstractBenchmark(MPI_Comm c, const std::string& addr, const snt::Client& client, const snt::Admin& admin)
    : m_comm(c)
    , m_server_addr(addr)
    , m_client(client)
    , m_admin(admin)
    {}

    virtual ~AbstractBenchmark() = default;
    virtual void setup()    = 0;
    virtual void execute()  = 0;
    virtual void teardown() = 0;

    /**
     * @brief Factory function used to create benchmark instances.
     */
    template<typename ... T>
    static std::unique_ptr<AbstractBenchmark> create(const std::string& type, T&& ... args) {
        auto it = s_benchmark_factories.find(type);
        if(it == s_benchmark_factories.end())
            throw std::invalid_argument(type+" benchmark type unknown");
        return (it->second)(std::forward<T>(args)...);
    }
};

/**
 * @brief The mechanism bellow is used to provide the REGISTER_BENCHMARK macro,
 * which registers a child class of AbstractBenchmark and allows AbstractBenchmark::create("type", ...)
 * to return an instance of this concrete child class.
 */
template<typename T>
class BenchmarkRegistration {
    public:
    BenchmarkRegistration(const std::string& type) {
        AbstractBenchmark::s_benchmark_factories[type] = 
            [](Json::Value& config, MPI_Comm comm,
                    const std::string& addr, const snt::Client& client, const snt::Admin& admin) {
                return std::make_unique<T>(config, comm, addr, client, admin);
        };
    }
};

std::map<std::string, AbstractBenchmark::benchmark_factory_function> AbstractBenchmark::s_benchmark_factories;

#define REGISTER_BENCHMARK(__name, __class) \
    static BenchmarkRegistration<__class> __class##_registration(__name)

/**
 * StoreBenchmark executes a series of store operations and measures their duration.
 */
class StoreBenchmark : public AbstractBenchmark {

    protected:

    RecordInfo      m_record_info;
    CollectionInfo  m_collection_info;
    snt::Collection m_collection;
    bool            m_use_json = false;
    std::vector<std::string> m_records;
    std::vector<Json::Value> m_records_json;

    public:

    template<typename ... T>
    StoreBenchmark(Json::Value& config, T&& ... args)
    : AbstractBenchmark(std::forward<T>(args)...)
    , m_record_info(config["records"])
    , m_collection_info(config["collection"]) 
    {
        m_use_json = config.get("use-json", false).asBool();
    }

    virtual void setup() override {
        m_collection = m_collection_info.createDatabaseAndCollection(client(), admin(), server_addr());
        if(!m_use_json) {
            m_records.reserve(m_record_info.num);
            for(size_t i = 0; i < m_record_info.num; i++) {
                std::string r;
                m_record_info.generateRandomRecordString(&r);
                m_records.push_back(std::move(r));
            }
        } else {
            m_records_json.reserve(m_record_info.num);
            for(size_t i = 0; i < m_record_info.num; i++) {
                Json::Value r;
                m_record_info.generateRandomRecordString(&r);
                m_records_json.push_back(std::move(r));
            }
        }
    }

    virtual void execute() override {
        if(!m_use_json) {
            for(auto& r : m_records) {
                m_collection.store(r);
            }
        } else {
            for(auto& r : m_records_json) {
                m_collection.store(r);
            }
        }
    }

    virtual void teardown() override {
        m_collection_info.eraseDatabaseAndCollection(client(), admin(), server_addr());
        m_records.clear();
        m_records_json.clear();
    }
};
REGISTER_BENCHMARK("store", StoreBenchmark);

/**
 * FetchBenchmark executes a series of fetch operations and measures their duration.
 */
class FetchBenchmark : public AbstractBenchmark {

    protected:

    RecordInfo      m_record_info;
    CollectionInfo  m_collection_info;
    snt::Collection m_collection;
    bool            m_use_json = false;
    size_t          m_num_fetch = 0; 

    public:

    template<typename ... T>
    FetchBenchmark(Json::Value& config, T&& ... args)
    : AbstractBenchmark(std::forward<T>(args)...)
    , m_record_info(config["records"])
    , m_collection_info(config["collection"]) 
    {
        m_use_json = config.get("use-json", false).asBool();
        if(!config["num-operations"]) {
            throw std::runtime_error("Fetch benchmark needs a num-operations parameter");
        }
        m_num_fetch = config["num-operations"].asUInt64();
    }

    virtual void setup() override {
        m_collection = m_collection_info.createDatabaseAndCollection(client(), admin(), server_addr());
        for(size_t i = 0; i < m_record_info.num; i++) {
            std::string r;
            m_record_info.generateRandomRecordString(&r);
            m_collection.store(r);
        }
    }

    virtual void execute() override {
        for(size_t i = 0; i < m_num_fetch; i++) {
            uint64_t id = rand() % m_record_info.num;
            if(!m_use_json) {
                std::string r;
                m_collection.fetch(id, &r);
            } else {
                Json::Value r;
                m_collection.fetch(id, &r);
            }
        }
    }

    virtual void teardown() override {
        m_collection_info.eraseDatabaseAndCollection(client(), admin(), server_addr());
    }
};
REGISTER_BENCHMARK("fetch", FetchBenchmark);

/**
 * FilterBenchmark executes a series of filter operations and measures their duration.
 */
class FilterBenchmark : public AbstractBenchmark {

    protected:

    RecordInfo      m_record_info;
    CollectionInfo  m_collection_info;
    snt::Collection m_collection;
    bool            m_use_json = false;
    double          m_selectivity = 0.0;
    std::string     m_function;

    public:

    template<typename ... T>
    FilterBenchmark(Json::Value& config, T&& ... args)
    : AbstractBenchmark(std::forward<T>(args)...)
    , m_record_info(config["records"])
    , m_collection_info(config["collection"]) 
    {
        m_use_json = config.get("use-json", false).asBool();
        if(!config["filter-selectivity"]) {
            throw std::runtime_error("Fetch benchmark needs a filter-selectivity parameter");
        }
        m_selectivity = config["filter-selectivity"].asDouble();
        std::stringstream ss;
        ss << "function($rec) { return $rec.__p__ < ";
        ss << std::setprecision(12) << m_selectivity;
        ss << "; }";
        m_function = ss.str();
    }

    virtual void setup() override {
        m_collection = m_collection_info.createDatabaseAndCollection(client(), admin(), server_addr());
        for(size_t i = 0; i < m_record_info.num; i++) {
            std::string r;
            m_record_info.generateRandomRecordString(&r);
            m_collection.store(r);
        }
    }

    virtual void execute() override {
        if(!m_use_json) {
            std::vector<std::string> result;
            m_collection.filter(m_function, &result);
        } else {
            Json::Value result;
            m_collection.filter(m_function, &result);
        }
    }

    virtual void teardown() override {
        m_collection_info.eraseDatabaseAndCollection(client(), admin(), server_addr());
    }
};
REGISTER_BENCHMARK("filter", FilterBenchmark);

/**
 * UpdateBenchmark executes a series of update operations and measures their duration.
 */
class UpdateBenchmark : public AbstractBenchmark {

    protected:

    RecordInfo      m_record_info;
    CollectionInfo  m_collection_info;
    snt::Collection m_collection;
    bool            m_use_json = false;
    size_t          m_num_updates = 0;
    std::vector<std::string> m_new_records;
    std::vector<Json::Value> m_new_records_json;

    public:

    template<typename ... T>
    UpdateBenchmark(Json::Value& config, T&& ... args)
    : AbstractBenchmark(std::forward<T>(args)...)
    , m_record_info(config["records"])
    , m_collection_info(config["collection"]) 
    {
        m_use_json = config.get("use-json", false).asBool();
        if(!config["num-operations"]) {
            throw std::runtime_error("Fetch benchmark needs a num-operations parameter");
        }
        m_num_updates = config["num-operations"].asUInt64();
    }

    virtual void setup() override {
        m_collection = m_collection_info.createDatabaseAndCollection(client(), admin(), server_addr());
        for(size_t i = 0; i < m_record_info.num; i++) {
            std::string r;
            m_record_info.generateRandomRecordString(&r);
            m_collection.store(r);
        }

        if(m_use_json) m_new_records_json.reserve(m_num_updates);
        else m_new_records.reserve(m_num_updates);

        for(size_t i = 0; i < m_num_updates; i++) {
            if(!m_use_json) {
                std::string r;
                m_record_info.generateRandomRecordString(&r);
                m_new_records.push_back(std::move(r));
            } else {
                Json::Value r;
                m_record_info.generateRandomRecordString(&r);
                m_new_records_json.push_back(std::move(r));
            }
        }
    }

    virtual void execute() override {
        for(size_t i = 0; i < m_num_updates; i++) {
            uint64_t id = rand() % m_record_info.num;
            if(!m_use_json) {
                m_collection.update(id, m_new_records[i]);
            } else {
                m_collection.update(id, m_new_records_json[i]);
            }
        }
    }

    virtual void teardown() override {
        m_collection_info.eraseDatabaseAndCollection(client(), admin(), server_addr());
    }
};
REGISTER_BENCHMARK("update", UpdateBenchmark);

/**
 * AllBenchmark executes a series of "all" operations and measures their duration.
 */
class AllBenchmark : public AbstractBenchmark {

    protected:

    RecordInfo      m_record_info;
    CollectionInfo  m_collection_info;
    snt::Collection m_collection;
    bool            m_use_json = false;

    public:

    template<typename ... T>
    AllBenchmark(Json::Value& config, T&& ... args)
    : AbstractBenchmark(std::forward<T>(args)...)
    , m_record_info(config["records"])
    , m_collection_info(config["collection"]) 
    {
        m_use_json = config.get("use-json", false).asBool();
    }

    virtual void setup() override {
        m_collection = m_collection_info.createDatabaseAndCollection(client(), admin(), server_addr());
        for(size_t i = 0; i < m_record_info.num; i++) {
            std::string r;
            m_record_info.generateRandomRecordString(&r);
            m_collection.store(r);
        }
    }

    virtual void execute() override {
        if(m_use_json) {
            Json::Value all;
            m_collection.all(&all);
        } else {
            std::vector<std::string> all;
            m_collection.all(&all);
        }
    }

    virtual void teardown() override {
        m_collection_info.eraseDatabaseAndCollection(client(), admin(), server_addr());
    }
};
REGISTER_BENCHMARK("all", AllBenchmark);

/**
 * EraseBenchmark executes a series of "erase" operations and measures their duration.
 */
class EraseBenchmark : public AbstractBenchmark {

    protected:

    RecordInfo      m_record_info;
    CollectionInfo  m_collection_info;
    snt::Collection m_collection;
    std::vector<uint64_t> m_erase_order;

    public:

    template<typename ... T>
    EraseBenchmark(Json::Value& config, T&& ... args)
    : AbstractBenchmark(std::forward<T>(args)...)
    , m_record_info(config["records"])
    , m_collection_info(config["collection"]) 
    {}

    virtual void setup() override {
        m_collection = m_collection_info.createDatabaseAndCollection(client(), admin(), server_addr());
        m_erase_order.resize(0);
        m_erase_order.reserve(m_record_info.num);
        for(size_t i = 0; i < m_record_info.num; i++) {
            std::string r;
            m_record_info.generateRandomRecordString(&r);
            m_collection.store(r);
            m_erase_order.push_back(i);
        }
        std::random_shuffle(m_erase_order.begin(), m_erase_order.end());
    }

    virtual void execute() override {
        for(size_t i = 0; i < m_erase_order.size(); i++) {
            m_collection.erase(m_erase_order[i]);
        }
    }

    virtual void teardown() override {
        m_collection_info.eraseDatabaseAndCollection(client(), admin(), server_addr());
    }
};
REGISTER_BENCHMARK("erase", EraseBenchmark);



static void run_server(MPI_Comm comm, Json::Value& config);
static void run_client(MPI_Comm comm, Json::Value& config);

/**
 * @brief Main function.
 */
int main(int argc, char** argv) {

    MPI_Init(&argc, &argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if(argc != 2) {
        if(rank == 0) {
            std::cerr << "Usage: " << argv[0] << " <config.json>" << std::endl;
            MPI_Abort(MPI_COMM_WORLD, -1);
        }
    }

    std::ifstream config_file(argv[1]);
    if(!config_file.good() && rank == 0) {
        std::cerr << "Could not read configuration file " << argv[1] << std::endl;
        MPI_Abort(MPI_COMM_WORLD, -1);
    }

    Json::CharReaderBuilder builder;
    Json::Value config;
    JSONCPP_STRING errs;
    if(!parseFromStream(builder, config_file, &config, &errs)) {
        std::cout << errs << std::endl;
        MPI_Abort(MPI_COMM_WORLD, -1);
        return -1;
    }

    MPI_Comm comm;
    MPI_Comm_split(MPI_COMM_WORLD, rank == 0 ? 0 : 1, rank, &comm);

    if(rank == 0) {
        run_server(comm, config);
    } else {
        run_client(comm, config);
    }

    MPI_Finalize();
    return 0;
}

static void run_server(MPI_Comm comm, Json::Value& config) {
    // initialize Thallium 
    std::string protocol = config["protocol"].asString();
    bool use_progress_thread = false;
    int  rpc_thread_count = 0;
    if(config.isMember("server")) {
        auto& server_config = config["server"];
        use_progress_thread = server_config.get("use-progress-thread", false).asBool();
        rpc_thread_count = server_config.get("rpc-thread-count", 0).asInt();
    }
    tl::engine engine(protocol, THALLIUM_SERVER_MODE, use_progress_thread, rpc_thread_count);
    engine.enable_remote_shutdown();
    // serialize server address
    std::string server_addr = engine.self();
    // send server address to client
    size_t server_addr_size = server_addr.size();
    MPI_Bcast(&server_addr_size, sizeof(server_addr_size), MPI_BYTE, 0, MPI_COMM_WORLD);
    MPI_Bcast(const_cast<char*>(server_addr.data()), server_addr_size, MPI_BYTE, 0, MPI_COMM_WORLD);
    // initialize Sonata provider
    snt::Provider provider(engine);
    // notify clients that the provider is ready
    MPI_Barrier(MPI_COMM_WORLD);
    // wait for finalize
    engine.wait_for_finalize();
}

static void run_client(MPI_Comm comm, Json::Value& config) {
    // get info from communicator
    int rank, num_clients;
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &num_clients);
    Json::StreamWriterBuilder builder;
    const std::unique_ptr<Json::StreamWriter> writer(builder.newStreamWriter());
    // initialize Thallium
    std::string protocol = config["protocol"].asString();
    bool use_progress_thread = false;
    if(config.isMember("client")) {
        auto& client_config = config["client"];
        use_progress_thread = client_config.get("use-progress-thread", false).asBool();
    }
    tl::engine engine(protocol, THALLIUM_CLIENT_MODE, use_progress_thread, 0);
    // receive server address
    std::string server_addr_str;
    size_t addr_size;
    MPI_Bcast(&addr_size, sizeof(size_t), MPI_BYTE, 0, MPI_COMM_WORLD);
    server_addr_str.resize(addr_size, '\0');
    MPI_Bcast(const_cast<char*>(server_addr_str.data()), addr_size, MPI_BYTE, 0, MPI_COMM_WORLD);
    // wait for server to have initialize the database
    MPI_Barrier(MPI_COMM_WORLD);
    {
        // open remote database
        snt::Client client(engine);
        snt::Admin admin(engine);
        // initialize the RNG seed
        int seed = config["seed"].asInt();
        // initialize benchmark instances
        std::vector<std::unique_ptr<AbstractBenchmark>> benchmarks;
        std::vector<unsigned> repetitions;
        std::vector<std::string> types;
        benchmarks.reserve(config["benchmarks"].size());
        repetitions.reserve(config["benchmarks"].size());
        types.reserve(config["benchmarks"].size());
        for(auto& bench_config : config["benchmarks"]) {
            std::string type = bench_config["type"].asString();
            types.push_back(type);
            benchmarks.push_back(AbstractBenchmark::create(type, bench_config, comm, server_addr_str, client, admin));
            repetitions.push_back(bench_config["repetitions"].asUInt());
        }
        // main execution loop
        for(unsigned i = 0; i < benchmarks.size(); i++) {
            auto& bench  = benchmarks[i];
            unsigned rep = repetitions[i];
            // reset the RNG
            srand(seed + rank*1789);
            std::vector<double> local_timings(rep);
            for(unsigned j = 0; j < rep; j++) {
                MPI_Barrier(comm);
                // benchmark setup
                bench->setup();
                MPI_Barrier(comm);
                // benchmark execution
                double t_start = MPI_Wtime();
                bench->execute();
                double t_end = MPI_Wtime();
                local_timings[j] = t_end - t_start;
                MPI_Barrier(comm);
                // teardown
                bench->teardown();
            }
            // exchange timings
            std::vector<double> global_timings(rep*num_clients);
            if(num_clients != 1) {
                MPI_Gather(local_timings.data(), local_timings.size(), MPI_DOUBLE,
                       global_timings.data(), local_timings.size(), MPI_DOUBLE, 0, comm);
            } else {
                std::copy(local_timings.begin(), local_timings.end(), global_timings.begin());
            }
            // print report
            if(rank == 0) {
                size_t n = global_timings.size();
                std::cout << "================ " << types[i] << " ================" << std::endl;
                writer->write(config["benchmarks"][i], &std::cout);
                std::cout << std::endl;
                std::cout << "-----------------" << std::string(types[i].size(),'-') << "-----------------" << std::endl;
                double average  = std::accumulate(global_timings.begin(), global_timings.end(), 0.0) / n;
                double variance = std::accumulate(global_timings.begin(), global_timings.end(), 0.0, [average](double acc, double x) {
                        return acc + std::pow((x - average),2);
                    });
                variance /= n;
                double stddev = std::sqrt(variance);
                std::sort(global_timings.begin(), global_timings.end());
                double min = global_timings[0];
                double max = global_timings[global_timings.size()-1];
                double median = (n % 2) ? global_timings[n/2] : ((global_timings[n/2] + global_timings[n/2 - 1])/2.0);
                double q1 = global_timings[n/4];
                double q3 = global_timings[(3*n)/4];
                std::cout << std::setprecision(9) << std::fixed;
                std::cout << "Samples         : " << n << std::endl;
                std::cout << "Average(sec)    : " << average << std::endl;
                std::cout << "Variance(sec^2) : " << variance << std::endl;
                std::cout << "StdDev(sec)     : " << stddev << std::endl;
                std::cout << "Minimum(sec)    : " << min << std::endl;
                std::cout << "Q1(sec)         : " << q1 << std::endl;
                std::cout << "Median(sec)     : " << median << std::endl;
                std::cout << "Q3(sec)         : " << q3 << std::endl;
                std::cout << "Maximum(sec)    : " << max << std::endl;
            }
        }
        // wait for all the clients to be done with their tasks
        MPI_Barrier(comm);
        // shutdown server and finalize margo
        if(rank == 0) {
            auto server_addr = engine.lookup(server_addr_str);
            engine.shutdown_remote_engine(server_addr);
        }
    }
}
