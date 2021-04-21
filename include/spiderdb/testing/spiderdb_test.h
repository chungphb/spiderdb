//
// Created by chungphb on 21/4/21.
//

#pragma once

#include <spiderdb/testing/test_runner.h>
#include <spiderdb/testing/test_logger.h>
#include <boost/test/unit_test.hpp>

namespace spiderdb {
namespace testing {

const char* const SPIDERDB_MASTER_TEST_SUITE = "spiderdb_master_test_suite";

struct spiderdb_test_case {
    spiderdb_test_case() = default;
    ~spiderdb_test_case() = default;
    virtual const char* get_test_file() = 0;
    virtual const char* get_name() = 0;
    virtual seastar::future<> run_test_case() = 0;
    virtual void run();
};

struct spiderdb_auto_test_case : public spiderdb_test_case {
    spiderdb_auto_test_case() = delete;
    spiderdb_auto_test_case(const char* suite_name);
};

int entry_point(int argc, char** argv);

}
}