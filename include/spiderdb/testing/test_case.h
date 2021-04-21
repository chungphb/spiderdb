//
// Created by chungphb on 21/4/21.
//

#pragma once

#include <spiderdb/testing/spiderdb_test.h>
#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>

using namespace spiderdb::testing;

#ifdef SPIDERDB_USING_MASTER_TEST_SUITE
namespace {
const char* spiderdb_current_test_suite = SPIDERDB_MASTER_TEST_SUITE;
}
#endif

#define SPIDERDB_TEST_SUITE(test_suite_name) \
namespace test_suite_name { \
namespace { \
const char* spiderdb_current_test_suite = #test_suite_name \
}

#define SPIDERDB_TEST_SUITE_END() \
}

#define SPIDERDB_TEST_CASE(test_case_name) \
struct test_case_name : public spiderdb_auto_test_case { \
    test_case_name() : spiderdb_auto_test_case(spiderdb_current_test_suite) {} \
    const char* get_test_file() override { \
        return __FILE__; \
    } \
    const char* get_name() { \
        return #test_case_name; \
    } \
    seastar::future<> run_test_case() override; \
}; \
static test_case_name test_case_name ## instance; \
seastar::future<> test_case_name::run_test_case()
