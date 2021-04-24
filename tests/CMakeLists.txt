cmake_minimum_required(VERSION 3.19)

set(CMAKE_CXX_STANDARD 17)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")

find_package(Boost COMPONENTS system filesystem unit_test_framework REQUIRED)
set(SEASTAR_TEST_HEADERS
    ${CMAKE_SOURCE_DIR}/modules/seastar/include/seastar/testing/test_runner.hh
    ${CMAKE_SOURCE_DIR}/modules/seastar/include/seastar/testing/exchanger.hh
    ${CMAKE_SOURCE_DIR}/modules/seastar/include/seastar/testing/random.hh
)
set(SEASTAR_TEST_SOURCES
    ${CMAKE_SOURCE_DIR}/modules/seastar/src/testing/test_runner.cc
    ${CMAKE_SOURCE_DIR}/modules/seastar/src/testing/random.cc
    ${CMAKE_SOURCE_DIR}/modules/seastar/src/testing/seastar_test.cc
)
set(SPIDERDB_TEST_HEADERS
    ${CMAKE_SOURCE_DIR}/include/spiderdb/testing/spiderdb_test.h
    ${CMAKE_SOURCE_DIR}/include/spiderdb/testing/test_case.h
    ${CMAKE_SOURCE_DIR}/include/spiderdb/testing/test_logger.h
    ${CMAKE_SOURCE_DIR}/include/spiderdb/testing/test_runner.h
)
set(SPIDERDB_TEST_SOURCES
    ${CMAKE_SOURCE_DIR}/src/testing/spiderdb_test.cpp
    ${CMAKE_SOURCE_DIR}/src/testing/test_runner.cpp
)
set(SPIDERDB_TESTS
    ${CMAKE_SOURCE_DIR}/tests/sample_test.cpp
    ${CMAKE_SOURCE_DIR}/tests/unit/string_test.cpp
    ${CMAKE_SOURCE_DIR}/tests/unit/cache_test.cpp
)
add_executable(spiderdb_test
    ${SEASTAR_TEST_HEADERS}
    ${SEASTAR_TEST_SOURCES}
    ${SPIDERDB_TEST_HEADERS}
    ${SPIDERDB_TEST_SOURCES}
    ${SPIDERDB_TESTS}
)
target_link_libraries(spiderdb_test
    spiderdb
    seastar
    ${Boost_SYSTEM_LIBRARY}
    ${Boost_FILESYSTEM_LIBRARY}
    ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY}
)