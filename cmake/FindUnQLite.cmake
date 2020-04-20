#
#  UnQLite_FOUND - system has UnQLite
#  UnQLite_INCLUDE_DIR - the UnQLite include directory
#  UnQLite_LIBRARIES - UnQLite library

set(UnQLite_LIBRARY_DIRS "")

find_library(UnQLite_LIBRARIES NAMES unqlite)
find_path(UnQLite_INCLUDE_DIRS NAMES unqlite.h)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(UnQLite DEFAULT_MSG UnQLite_LIBRARIES UnQLite_INCLUDE_DIRS)
