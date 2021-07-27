include_directories(${CMAKE_SOURCE_DIR}/include)

add_executable(01_server 01_server.cpp)
target_link_libraries(01_server sonata-server)

add_executable(02_admin 02_admin.cpp)
target_link_libraries(02_admin sonata-admin)

add_executable(03_client 03_client.cpp)
target_link_libraries(03_client sonata-admin sonata-client)

add_executable(04_database 04_database.cpp)
target_link_libraries(04_database sonata-admin sonata-client)

add_executable(05_collection 05_collection.cpp)
target_link_libraries(05_collection sonata-admin sonata-client)

add_executable(06_exec 06_exec.cpp)
target_link_libraries(06_exec sonata-admin sonata-client)

add_subdirectory(provdb)
