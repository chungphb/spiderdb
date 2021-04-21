//
// Created by chungphb on 16/4/21.
//

#define SPIDERDB_USING_MASTER_TEST_SUITE
#include <spiderdb/testing/test_case.h>

SPIDERDB_TEST_SUITE(sample_test_suite)

SPIDERDB_TEST_CASE(sample_test_case) {
    SPIDERDB_TEST_MESSAGE("Hello world!");
    SPIDERDB_CHECK(true);
    return seastar::now();
}

SPIDERDB_TEST_SUITE_END()
