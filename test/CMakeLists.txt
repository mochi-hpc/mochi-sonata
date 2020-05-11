include_directories(../include)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/test.json ${CMAKE_CURRENT_BINARY_DIR}/test.json COPYONLY)
file(GLOB JX9_TEST_FILES ${CMAKE_CURRENT_SOURCE_DIR}/*.jx9)
foreach(file ${JX9_TEST_FILES})
	get_filename_component(filename ${file} NAME)
	message(STATUS "Found Jx9 test file ${filename}")
	configure_file(${CMAKE_CURRENT_SOURCE_DIR}/${filename} ${CMAKE_CURRENT_BINARY_DIR}/${filename} COPYONLY)
endforeach()

add_library(sonata-test Main.cpp)
target_link_libraries(sonata-test cppunit sonata-server sonata-admin sonata-client)

add_executable(AdminTest AdminTest.cpp)
target_link_libraries(AdminTest sonata-test)

add_executable(ClientTest AdminTest.cpp)
target_link_libraries(ClientTest sonata-test)

add_executable(DatabaseTest DatabaseTest.cpp)
target_link_libraries(DatabaseTest sonata-test)

add_executable(CollectionTest CollectionTest.cpp)
target_link_libraries(CollectionTest sonata-test)

add_executable(CollectionMultiTest CollectionMultiTest.cpp)
target_link_libraries(CollectionMultiTest sonata-test)

add_executable(ExecTest ExecTest.cpp)
target_link_libraries(ExecTest sonata-test)

add_executable(Jx9Test Jx9Test.cpp)
target_link_libraries(Jx9Test sonata-test)

add_test(NAME AdminTestUnQLite COMMAND ./AdminTest AdminTestUnQLite.xml unqlite)
add_test(NAME AdminTestJsonCpp COMMAND ./AdminTest AdminTestJsonCpp.xml jsoncpp)

add_test(NAME ClientTestUnQLite COMMAND ./ClientTest ClientTestUnQLite.xml unqlite)
add_test(NAME ClientTestJsonCpp COMMAND ./ClientTest ClientTestJsonCpp.xml jsoncpp)

add_test(NAME DatabaseTestUnQLite COMMAND ./DatabaseTest DatabaseTestUnQlite.xml unqlite)
add_test(NAME DatabaseTestJsonCpp COMMAND ./DatabaseTest DatabaseTestJsonCpp.xml jsoncpp)

add_test(NAME CollectionTestUnQLite COMMAND ./CollectionTest CollectionTestUnQLite.xml unqlite)
add_test(NAME CollectionTestJsonCpp COMMAND ./CollectionTest CollectionTestJsonCpp.xml jsoncpp)

add_test(NAME CollectionMultiTestUnQLite COMMAND ./CollectionMultiTest CollectionMultiTestUnQLite.xml unqlite)
add_test(NAME CollectionMultiTestJsonCpp COMMAND ./CollectionMultiTest CollectionMultiTestJsonCpp.xml jsoncpp)

add_test(NAME ExecTest COMMAND ./ExecTest ExecTest.xml)

add_test(NAME Jx9Test COMMAND ./Jx9Test Jx9Test.xml)
