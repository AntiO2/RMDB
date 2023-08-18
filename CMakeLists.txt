cmake_minimum_required(VERSION 3.16)
project(RMDB)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-Wall -O0 -g -ggdb3")
## set(CMAKE_CXX_FLAGS "-Wall -O3")
#
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0 -g")

enable_testing()
add_subdirectory(src)
add_subdirectory(deps)
add_subdirectory(test)
# add_subdirectory(rmdb_client)

SET(GDB_INCLUDE_DIRS
        ${PROJECT_SOURCE_DIR}/deps/fmt/include)
MESSAGE(INCLUDE ${GDB_INCLUDE_DIRS})
INCLUDE_DIRECTORIES(GDB_INCLUDE_DIRS)