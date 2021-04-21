//
// Created by chungphb on 16/4/21.
//

#define SPIDERDB_USING_MASTER_TEST_SUITE
#include <spiderdb/testing/test_case.h>

SPIDERDB_TEST_CASE(sample) {
    SPIDERDB_TEST_MESSAGE("Hello world!");
    return seastar::now();
}