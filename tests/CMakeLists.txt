cmake_minimum_required(VERSION 3.19)

set(CMAKE_CXX_STANDARD 17)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

find_package(Boost COMPONENTS system filesystem unit_test_framework REQUIRED)
set(SEASTAR_TESTING_HDRS
        ${CMAKE_SOURCE_DIR}/modules/seastar/include/seastar/testing/test_runner.hh
        ${CMAKE_SOURCE_DIR}/modules/seastar/include/seastar/testing/exchanger.hh
        ${CMAKE_SOURCE_DIR}/modules/seastar/include/seastar/testing/random.hh)
set(SEASTAR_TESTING_SRCS
        ${CMAKE_SOURCE_DIR}/modules/seastar/src/testing/test_runner.cc
        ${CMAKE_SOURCE_DIR}/modules/seastar/src/testing/random.cc
        ${CMAKE_SOURCE_DIR}/modules/seastar/src/testing/seastar_test.cc)
set(SPIDERDB_TESTING_HDRS
        ${CMAKE_SOURCE_DIR}/include/spiderdb/testing/spiderdb_test.h
        ${CMAKE_SOURCE_DIR}/include/spiderdb/testing/test_case.h
        ${CMAKE_SOURCE_DIR}/include/spiderdb/testing/test_logger.h
        ${CMAKE_SOURCE_DIR}/include/spiderdb/testing/test_runner.h)
set(SPIDERDB_TESTING_SRCS
        ${CMAKE_SOURCE_DIR}/src/testing/spiderdb_test.cpp
        ${CMAKE_SOURCE_DIR}/src/testing/test_runner.cpp)
add_library(spiderdb_testing
        ${SEASTAR_TESTING_HDRS}
        ${SEASTAR_TESTING_SRCS}
        ${SPIDERDB_TESTING_HDRS}
        ${SPIDERDB_TESTING_SRCS})
target_link_libraries(spiderdb_testing
        seastar
        ${Boost_SYSTEM_LIBRARY}
        ${Boost_FILESYSTEM_LIBRARY}
        ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY})

# String tests
add_executable(spiderdb_string_test
        ${CMAKE_SOURCE_DIR}/tests/unit/string_test.cpp)
target_link_libraries(spiderdb_string_test
        spiderdb_testing)

# Cache tests
add_executable(spiderdb_cache_test
        ${CMAKE_SOURCE_DIR}/tests/unit/cache_test.cpp
        ${CMAKE_SOURCE_DIR}/src/util/error.cpp)
target_link_libraries(spiderdb_cache_test
        spiderdb_testing)

# File tests, B-Tree tests, Storage tests
foreach(target_var file btree storage)
    add_executable("spiderdb_${target_var}_test"
            "${CMAKE_SOURCE_DIR}/tests/unit/${target_var}_test.cpp")
    target_link_libraries("spiderdb_${target_var}_test"
            spiderdb_testing
            "spiderdb_${target_var}")
endforeach()