# list of source files
set(sonata-client-src
	Client.cpp
	Database.cpp
	Collection.cpp
	AsyncRequest.cpp
)
set(sonata-server-src
	Provider.cpp
	Backend.cpp
	LazyBackend.cpp
	JsonCppBackend.cpp
	VectorBackend.cpp
	NullBackend.cpp
	UnQLiteBackend.cpp
	UnQLiteVM.cpp
	UnQLiteMutex.cpp
	unqlite/unqlite.c
)
set(sonata-admin-src
	Admin.cpp
)
set(sonata-bedrock-src
	BedrockModule.cpp
)

if(${ENABLE_UNQLITE_THREADS})
  add_definitions("-DUNQLITE_ENABLE_THREADS")
endif(${ENABLE_UNQLITE_THREADS})

# load package helper for generating cmake CONFIG packages
include (CMakePackageConfigHelpers)

# where to install files for "find_package"
set (sonata-pkg "share/cmake/sonata")

#
# library version set here (e.g. for shared libs).
#
set (SONATA_VERSION_MAJOR 0)
set (SONATA_VERSION_MINOR 6)
set (SONATA_VERSION_PATCH 0)
set (sonata-vers "${SONATA_VERSION_MAJOR}.${SONATA_VERSION_MINOR}")
set (SONATA_VERSION "${sonata-vers}.${SONATA_VERSION_PATCH}")

add_library(sonata-client ${sonata-client-src})
target_link_libraries (sonata-client thallium PkgConfig::margo PkgConfig::jsoncpp spdlog::spdlog cereal)
target_include_directories (sonata-client PUBLIC $<INSTALL_INTERFACE:include>)

add_library(sonata-admin ${sonata-admin-src})
target_link_libraries (sonata-admin thallium PkgConfig::margo PkgConfig::jsoncpp spdlog::spdlog cereal)
target_include_directories (sonata-admin PUBLIC $<INSTALL_INTERFACE:include>)

add_library(sonata-server ${sonata-server-src})
target_link_libraries (sonata-server sonata-client sonata-admin thallium PkgConfig::margo PkgConfig::jsoncpp spdlog::spdlog cereal)
target_include_directories (sonata-server PUBLIC $<INSTALL_INTERFACE:include>)

if(${ENABLE_BEDROCK})
add_library(sonata-bedrock ${sonata-bedrock-src})
target_link_libraries (sonata-bedrock sonata-server sonata-client sonata-admin cereal bedrock-server)
target_include_directories (sonata-bedrock PUBLIC $<INSTALL_INTERFACE:include>)
endif()

# local include's BEFORE, in case old incompatable .h files in prefix/include
target_include_directories (sonata-client BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (sonata-admin BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (sonata-server BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
if(${ENABLE_BEDROCK})
target_include_directories (sonata-bedrock BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
endif()

# for shared libs, establish the lib version
set_target_properties (sonata-client
    PROPERTIES VERSION ${SONATA_VERSION}
    SOVERSION ${SONATA_VERSION_MAJOR})

set_target_properties (sonata-admin
    PROPERTIES VERSION ${SONATA_VERSION}
    SOVERSION ${SONATA_VERSION_MAJOR})

set_target_properties (sonata-server
    PROPERTIES VERSION ${SONATA_VERSION}
    SOVERSION ${SONATA_VERSION_MAJOR})

if(${ENABLE_BEDROCK})
set_target_properties (sonata-bedrock
    PROPERTIES VERSION ${SONATA_VERSION}
    SOVERSION ${SONATA_VERSION_MAJOR})
endif()

#
# installation stuff (packaging and install commands)
#
write_basic_package_version_file(
    "sonata-config-version.cmake"
    VERSION ${SONATA_VERSION}
    COMPATIBILITY AnyNewerVersion)

# generate our config file for find_package()
configure_file (sonata-config.cmake.in sonata-config.cmake @ONLY)

# some bits for the pkg-config file
set (DEST_DIR "${CMAKE_INSTALL_PREFIX}")
set (PRIVATE_CLIENT_LIBS "-lsonata-client")
set (PRIVATE_SERVER_LIBS "-lsonata-server")
set (PRIVATE_ADMIN_LIBS  "-lsonata-admin")
configure_file ("sonata-client.pc.in" "sonata-client.pc" @ONLY)
configure_file ("sonata-server.pc.in" "sonata-server.pc" @ONLY)
configure_file ("sonata-admin.pc.in" "sonata-admin.pc" @ONLY)

#
# "make install" rules
#
install (TARGETS sonata-client sonata-server sonata-admin EXPORT sonata-targets
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
if(${ENABLE_BEDROCK})
install (TARGETS sonata-bedrock
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib)
endif()
install (EXPORT sonata-targets
         DESTINATION ${sonata-pkg}
         FILE "sonata-targets.cmake")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/sonata-config.cmake"
               "${CMAKE_CURRENT_BINARY_DIR}/sonata-config-version.cmake"
         DESTINATION ${sonata-pkg} )
install (DIRECTORY ../include/sonata
         DESTINATION include
         FILES_MATCHING PATTERN "*.h")
install (DIRECTORY ../include/sonata
         DESTINATION include
         FILES_MATCHING PATTERN "*.hpp")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/sonata-client.pc"
         DESTINATION "lib/pkgconfig/")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/sonata-admin.pc"
         DESTINATION "lib/pkgconfig/")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/sonata-server.pc"
         DESTINATION "lib/pkgconfig/")
