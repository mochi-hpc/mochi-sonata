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

add_executable(ExecTest ExecTest.cpp)
target_link_libraries(ExecTest sonata-test)

add_executable(Jx9Test Jx9Test.cpp)
target_link_libraries(Jx9Test sonata-test)

add_test(NAME AdminTest COMMAND ./AdminTest AdminTest.xml)
add_test(NAME ClientTest COMMAND ./ClientTest ClientTest.xml)
add_test(NAME DatabaseTest COMMAND ./DatabaseTest DatabaseTest.xml)
add_test(NAME CollectionTest COMMAND ./CollectionTest CollectionTest.xml)
add_test(NAME ExecTest COMMAND ./ExecTest ExecTest.xml)
add_test(NAME Jx9Test COMMAND ./Jx9Test Jx9Test.xml)
