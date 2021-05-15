cmake_minimum_required(VERSION 3.19)

project(spiderdb)

set(CMAKE_CXX_STANDARD 17)

option(SpiderDB_TESTING "Enable testing targets" ON)

add_subdirectory(modules)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/modules/seastar/include)

if(SpiderDB_TESTING)
    add_definitions(-DSPIDERDB_TESTING)
endif()

set(SPIDERDB_HEADERS
    "include/spiderdb/core/data_page.h"
    "include/spiderdb/core/storage.h"
    "include/spiderdb/core/node.h"
    "include/spiderdb/core/btree.h"
    "include/spiderdb/core/page.h"
    "include/spiderdb/core/file.h"
    "include/spiderdb/core/config.h"
    "include/spiderdb/util/string.h"
    "include/spiderdb/util/cache.h"
    "include/spiderdb/util/log.h"
    "include/spiderdb/util/error.h"
    "include/spiderdb/util/data_types.h"
)
set(SPIDERDB_SOURCES
    "src/core/data_page.cpp"
    "src/core/storage.cpp"
    "src/core/node.cpp"
    "src/core/btree.cpp"
    "src/core/page.cpp"
    "src/core/file.cpp"
    "src/util/log.cpp"
    "src/util/error.cpp"
)
add_library(spiderdb STATIC ${SPIDERDB_HEADERS} ${SPIDERDB_SOURCES})
target_include_directories(spiderdb PUBLIC include)
target_link_libraries(spiderdb PUBLIC seastar)

if (SpiderDB_TESTING)
    add_subdirectory(tests)
endif()