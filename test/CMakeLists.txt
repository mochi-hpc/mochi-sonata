add_executable(mytest test.cpp)
target_include_directories(mytest PUBLIC $<INSTALL_INTERFACE:include>)
target_include_directories(mytest BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_link_libraries(mytest sonata-admin sonata-server sonata-client)
