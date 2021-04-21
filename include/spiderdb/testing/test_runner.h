//
// Created by chungphb on 21/4/21.
//

#pragma once

#include <seastar/testing/test_runner.hh>

namespace spiderdb {
namespace testing {

using spiderdb_test_runner = seastar::testing::test_runner;

spiderdb_test_runner& test_runner();

}
}