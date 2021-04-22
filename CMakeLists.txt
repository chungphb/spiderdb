cmake_minimum_required(VERSION 3.19)

project(spiderdb)

set(CMAKE_CXX_STANDARD 17)

option(SpiderDB_TESTING "Enable testing targets" ON)

add_subdirectory(modules)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/modules/seastar/include)

set(SPIDERDB_HEADERS
    "include/spiderdb/core/spiderdb.h"
    "include/spiderdb/util/string.h"
    "include/spiderdb/util/cache.h"
)
set(SPIDERDB_SOURCES
    "src/core/spiderdb.cpp"
)
add_library(spiderdb STATIC ${SPIDERDB_HEADERS} ${SPIDERDB_SOURCES})
target_include_directories(spiderdb PUBLIC include)
target_link_directories(spiderdb PRIVATE seastar)

if (SpiderDB_TESTING)
    add_subdirectory(tests)
endif()