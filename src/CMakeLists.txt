# list of source files
set(sonata-client-src Client.cpp Database.cpp Collection.cpp)
set(sonata-server-src Provider.cpp Backend.cpp UnQLiteBackend.cpp UnQLiteVM.cpp UnQLiteValue.cpp)
set(sonata-admin-src  Admin.cpp)

# load package helper for generating cmake CONFIG packages
include (CMakePackageConfigHelpers)

# where to install files for "find_package"
set (sonata-pkg "share/cmake/sonata")

#
# library version set here (e.g. for shared libs).
#
set (SONATA_VERSION_MAJOR 0)
set (SONATA_VERSION_MINOR 1)
set (SONATA_VERSION_PATCH 0)
set (sonata-vers "${SONATA_VERSION_MAJOR}.${SONATA_VERSION_MINOR}")
set (SONATA_VERSION "${sonata-vers}.${SONATA_VERSION_PATCH}")

add_library(sonata-client ${sonata-client-src})
target_link_libraries (sonata-client thallium margo jsoncpp spdlog)
target_include_directories (sonata-client PUBLIC $<INSTALL_INTERFACE:include>)

add_library(sonata-admin ${sonata-admin-src})
target_link_libraries (sonata-admin thallium margo jsoncpp spdlog)
target_include_directories (sonata-admin PUBLIC $<INSTALL_INTERFACE:include>)

add_library(sonata-server ${sonata-server-src})
target_link_libraries (sonata-server thallium margo unqlite jsoncpp spdlog)
target_include_directories (sonata-server PUBLIC $<INSTALL_INTERFACE:include>)

# local include's BEFORE, in case old incompatable .h files in prefix/include
target_include_directories (sonata-client BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (sonata-admin BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)
target_include_directories (sonata-server BEFORE PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>)

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

#
# installation stuff (packaging and install commands)
#
write_basic_package_version_file(
    "sonata-config-version.cmake"
    VERSION ${SONATA_VERSION}
    COMPATIBILITY AnyNewerVersion)

# generate our config file for find_package()
configure_file (sonata-config.cmake.in sonata-config.cmake @ONLY)

get_filename_component(UNQLITE_LIBDIR ${UNQLITE_LIBRARIES} DIRECTORY)

# some bits for the pkg-config file
set (DEST_DIR "${CMAKE_INSTALL_PREFIX}")
set (PRIVATE_CLIENT_LIBS "-lsonata-client")
set (PRIVATE_SERVER_LIBS "-lsonata-server -L${UNQLITE_LIBDIR} -lunqlite")
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
install (EXPORT sonata-targets
         DESTINATION ${sonata-pkg}
         FILE "sonata-targets.cmake")
install (FILES "${CMAKE_CURRENT_BINARY_DIR}/sonata-config.cmake"
               "${CMAKE_CURRENT_BINARY_DIR}/sonata-config-version.cmake"
               "../cmake/xpkg-import.cmake"
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
