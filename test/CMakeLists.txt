include_directories(../include)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/test.json ${CMAKE_CURRENT_BINARY_DIR}/test.json COPYONLY)

add_library(sonata-test Main.cpp)
target_link_libraries(sonata-test cppunit sonata-server sonata-admin sonata-client)

add_executable(DatabaseTest DatabaseTest.cpp)
target_link_libraries(DatabaseTest sonata-test)

add_test(NAME DatabaseTest COMMAND ./DatabaseTest)
