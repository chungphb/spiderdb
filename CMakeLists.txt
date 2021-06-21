cmake_minimum_required(VERSION 3.19)

project(spiderdb)

set(CMAKE_CXX_STANDARD 17)

option(SpiderDB_TESTING "Enable testing targets" ON)

add_subdirectory(modules)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/modules/seastar/include)

if(SpiderDB_TESTING)
    add_definitions(-DSPIDERDB_TESTING)
endif()

# File library
set(SPIDERDB_FILE_HDRS
        "include/spiderdb/core/file.h"
        "include/spiderdb/core/page.h"
        "include/spiderdb/core/config.h"
        "include/spiderdb/util/string.h"
        "include/spiderdb/util/log.h"
        "include/spiderdb/util/error.h"
        "include/spiderdb/util/data_types.h")
set(SPIDERDB_FILE_SRCS
        "src/core/file.cpp"
        "src/core/page.cpp"
        "src/util/log.cpp"
        "src/util/error.cpp")
add_library(spiderdb_file STATIC
        ${SPIDERDB_FILE_HDRS}
        ${SPIDERDB_FILE_SRCS})
target_link_libraries(spiderdb_file PUBLIC seastar)

# B-Tree library
set(SPIDERDB_BTREE_HDRS
        "include/spiderdb/core/btree.h"
        "include/spiderdb/core/node.h"
        "include/spiderdb/util/cache.h")
set(SPIDERDB_BTREE_SRCS
        "src/core/btree.cpp"
        "src/core/node.cpp")
add_library(spiderdb_btree STATIC
        ${SPIDERDB_BTREE_HDRS}
        ${SPIDERDB_BTREE_SRCS})
target_link_libraries(spiderdb_btree PUBLIC spiderdb_file)

# Storage library
set(SPIDERDB_STORAGE_HDRS
        "include/spiderdb/core/storage.h"
        "include/spiderdb/core/data_page.h")
set(SPIDERDB_STORAGE_SRCS
        "src/core/storage.cpp"
        "src/core/data_page.cpp")
add_library(spiderdb_storage STATIC
        ${SPIDERDB_STORAGE_HDRS}
        ${SPIDERDB_STORAGE_SRCS})
target_link_libraries(spiderdb_storage PUBLIC spiderdb_btree)

# SpiderDB library
set(SPIDERDB_HDRS
        "include/spiderdb/core/spiderdb.h"
        "include/spiderdb/util/hasher.h")
set(SPIDERDB_SRCS
        "src/core/spiderdb.cpp"
        "src/util/hasher.cpp")
add_library(spiderdb STATIC
        ${SPIDERDB_HDRS}
        ${SPIDERDB_SRCS})
target_link_libraries(spiderdb PUBLIC spiderdb_storage)

if (SpiderDB_TESTING)
    add_subdirectory(tests)
endif()