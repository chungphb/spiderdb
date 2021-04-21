//
// Created by chungphb on 21/4/21.
//

#define BOOST_TEST_ALTERNATIVE_INIT_API
#include <spiderdb/testing/spiderdb_test.h>
#include <seastar/core/app-template.hh>

namespace spiderdb {
namespace testing {

void spiderdb_test_case::run() {
    SPIDERDB_START_TEST_CASE();
    test_runner().run_sync([this] {
        return run_test_case();
    });
    SPIDERDB_FINISH_TEST_CASE();
}

static std::unordered_map<std::string, std::vector<spiderdb_auto_test_case*>*>* test_suites;

spiderdb_auto_test_case::spiderdb_auto_test_case(const char* suite_name) {
    if (!test_suites) {
        test_suites = new std::unordered_map<std::string, std::vector<spiderdb_auto_test_case*>*>();
    }
    auto it = test_suites->find(suite_name);
    if (it == test_suites->end()) {
        auto* tests = new std::vector<spiderdb_auto_test_case*>();
        auto result = test_suites->emplace(suite_name, tests);
        it = result.first;
    }
    it->second->push_back(this);
}

static bool init_unit_test_suite() {
    auto& master_test_suite = boost::unit_test::framework::master_test_suite();
    master_test_suite.p_name.set("spiderdb");
    for (auto& test_suite : *test_suites) {
        boost::unit_test::test_suite* tests;
        if (test_suite.first != SPIDERDB_MASTER_TEST_SUITE) {
            tests = BOOST_TEST_SUITE(test_suite.first.c_str());
        } else {
            tests = &master_test_suite;
        }
        for (auto* test : *test_suite.second) {
            #if BOOST_VERSION > 105800
                tests->add(boost::unit_test::make_test_case([test] { test->run(); }, test->get_name(), test->get_test_file(), 0), 0, 0);
            #else
                tests->add(boost::unit_test::make_test_case([test] { test->run(); }, test->get_name()), 0, 0));
            #endif
        }
        if (test_suite.first != SPIDERDB_MASTER_TEST_SUITE) {
            master_test_suite.add(tests);
        }
    }
    test_runner().start(master_test_suite.argc, master_test_suite.argv);
    return true;
}

int entry_point(int argc, char** argv) {
    const int exit_code = boost::unit_test::unit_test_main(&init_unit_test_suite, argc, argv);
    test_runner().finalize();
    return exit_code;
}

}
}

int main(int argc, char** argv) {
    return spiderdb::testing::entry_point(argc, argv);
}