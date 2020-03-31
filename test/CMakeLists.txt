include_directories(../include)

add_library(sonata-test Main.cpp)
target_link_libraries(sonata-test ${CPPUNIT_LIBRARIES} sonata-server sonata-admin sonata-client)

