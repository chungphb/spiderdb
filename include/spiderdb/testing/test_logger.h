//
// Created by chungphb on 21/4/21.
//

#pragma once

#include <fmt/format.h>

#define SPIDERDB_START_TEST_CASE() \
do { \
    auto message = fmt::format(">>>> START TEST CASE  {}", get_name()); \
    BOOST_TEST_MESSAGE(message); \
} while (false)

#define SPIDERDB_FINISH_TEST_CASE() \
do { \
    auto message = fmt::format(">>>> FINISH TEST CASE {}\n", get_name()); \
    BOOST_TEST_MESSAGE(message); \
} while (false)

#define SPIDERDB_WARN(predicate) BOOST_WARN(predicate)

#define SPIDERDB_CHECK(predicate) BOOST_CHECK(predicate)

#define SPIDERDB_REQUIRE(predicate) BOOST_REQUIRE(predicate)

#define SPIDERDB_WARN_MESSAGE(predicate, ...) \
do { \
    auto message = fmt::format(__VA_ARGS__); \
    BOOST_WARN_MESSAGE(predicate, message); \
} while (false)

#define SPIDERDB_CHECK_MESSAGE(predicate, ...) \
do { \
    auto message = fmt::format(__VA_ARGS__); \
    BOOST_CHECK_MESSAGE(predicate, message); \
} while (false)

#define SPIDERDB_REQUIRE_MESSAGE(predicate, ...) \
do { \
    auto message = fmt::format(__VA_ARGS__); \
    BOOST_REQUIRE_MESSAGE(predicate, message); \
} while (false)

#define SPIDERDB_TEST_MESSAGE(...) \
do { \
    auto message = fmt::format(__VA_ARGS__); \
    BOOST_TEST_MESSAGE(message); \
} while (false)
