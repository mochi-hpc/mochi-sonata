add_executable (sonata-daemon SonataDaemon.cpp)
target_include_directories (sonata-daemon PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories (sonata-daemon BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_link_libraries (sonata-daemon sonata-server jsoncpp)

add_executable (sonata-benchmark Benchmark.cpp)
#target_include_directories (sonata-benchmark PUBLIC $<INSTALL_INTERFACE:include>)
#target_include_directories (sonata-benchmark BEFORE PUBLIC
#    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_link_libraries(sonata-benchmark sonata-server sonata-client sonata-admin jsoncpp)
