find_package(MPI REQUIRED)

add_executable(provdb_server provdb_server.cpp)
target_link_libraries(provdb_server sonata-server MPI::MPI_C spdlog::spdlog)

add_executable(provdb_client provdb_client.cpp)
target_link_libraries(provdb_client sonata-admin sonata-client MPI::MPI_C spdlog::spdlog)
