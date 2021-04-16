cmake_minimum_required(VERSION 3.19)

project(spiderdb)

set(CMAKE_CXX_STANDARD 17)

option(SpiderDB_TESTING "Enable testing targets" ON)

set(SPIDERDB_HEADERS
    "include/spiderdb/core/spiderdb.h"
)
set(SPIDERDB_SOURCES
    "src/core/spiderdb.cpp"
)
add_library(spiderdb STATIC ${SPIDERDB_HEADERS} ${SPIDERDB_SOURCES})
target_include_directories(spiderdb PUBLIC include)

if (SpiderDB_TESTING)
    add_subdirectory(test)
endif()