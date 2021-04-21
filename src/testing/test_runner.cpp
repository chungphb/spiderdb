//
// Created by chungphb on 21/4/21.
//

#include <spiderdb/testing/test_runner.h>

namespace spiderdb {
namespace testing {

static spiderdb_test_runner instance;

spiderdb_test_runner& test_runner() {
    return instance;
}

}
}