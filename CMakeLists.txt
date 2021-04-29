#
#  general cmake flags:
#    -DCMAKE_INSTALL_PREFIX=/usr/local     -- the prefix for installing
#    -DCMAKE_BUILD_TYPE=type               -- type can be Debug, Release, ...
#    -DCMAKE_PREFIX_PATH=/dir              -- external packages
#
#     note that CMAKE_PREFIX_PATH can be a list of directories:
#      -DCMAKE_PREFIX_PATH='/dir1;/dir2;/dir3'
#

cmake_minimum_required (VERSION 3.12)
project (sonata C CXX)
enable_testing ()
set (CMAKE_CXX_STANDARD 14)

add_definitions(-g)
option(ENABLE_UNQLITE_THREADS "Enable thread-safe UnQLite" ON)
option(ENABLE_TESTS     "Build tests. May require CppUnit_ROOT" OFF)
option(ENABLE_EXAMPLES  "Build examples" OFF)
option(ENABLE_BENCHMARK "Build benchmark" OFF)
option(ENABLE_DAEMON    "Build default daemon" OFF)
option(ENABLE_BEDROCK   "Build the Bedrock module" ON)

# add our cmake module directory to the path
set (CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
     "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# link shared lib with full rpath
set (CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
set (CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# setup cache variables for ccmake
if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Release
         CACHE STRING "Choose the type of build." FORCE)
    set_property (CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS 
                  "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif ()
set (CMAKE_PREFIX_PATH "" CACHE STRING "External dependencies path")
set (BUILD_SHARED_LIBS "ON" CACHE BOOL "Build a shared library")

# packages we depend on
include (xpkg-import)
find_package (thallium REQUIRED)
find_package (cereal REQUIRED)

xpkg_import_module (margo REQUIRED margo)
xpkg_import_module (jsoncpp REQUIRED jsoncpp)
find_package (TCLAP REQUIRED)
find_package(spdlog REQUIRED)
if(${ENABLE_BEDROCK})
	find_package(bedrock REQUIRED)
endif()

include_directories(${UnQLite_INCLUDE_DIR})

find_package (CppUnit)
if (CPPUNIT_FOUND)
    message(STATUS "CppUnit found, unit tests will be compiled")
    include_directories(${CPPUNIT_INCLUDE_DIR})
    enable_testing()
    if(${ENABLE_TESTS})
        add_subdirectory (test)
    endif(${ENABLE_TESTS})
else (CPPUNIT_FOUND)
    message(STATUS "CppUnit not found, unit tests will not be compiled")
endif (CPPUNIT_FOUND)

add_subdirectory (src)
add_subdirectory (bin)
if(${ENABLE_EXAMPLES})
  add_subdirectory (examples)
endif(${ENABLE_EXAMPLES})
