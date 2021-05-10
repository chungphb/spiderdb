//
// Created by chungphb on 10/5/21.
//

#define SPIDERDB_USING_MASTER_TEST_SUITE
#include <spiderdb/core/btree.h>
#include <spiderdb/util/error.h>
#include <spiderdb/testing/test_case.h>
#include <boost/iterator/counting_iterator.hpp>

namespace {

const std::string DATA_FOLDER = "data";
const std::string DATA_FILE = DATA_FOLDER + "/test.dat";

struct btree_test_fixture {
    btree_test_fixture() {
        system(fmt::format("rm {}", DATA_FILE).c_str());
    }
    ~btree_test_fixture() = default;
};

}

SPIDERDB_TEST_SUITE(btree_test_open_and_close)

SPIDERDB_FIXTURE_TEST_CASE(test_one_btree_open_then_close, btree_test_fixture) {
    spiderdb::btree btree{DATA_FILE};
    return btree.open().then([btree] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_one_btree_open_without_closing, btree_test_fixture) {
    spiderdb::btree btree{DATA_FILE};
    return btree.open().finally([btree] {});
}

SPIDERDB_FIXTURE_TEST_CASE(test_one_btree_close_without_opening, btree_test_fixture) {
    spiderdb::btree btree{DATA_FILE};
    return btree.close().handle_exception([btree](auto ex) {
        try {
            std::rethrow_exception(ex);
        } catch (spiderdb::spiderdb_error& err) {
            SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
        }
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_one_btree_multiple_consecutive_opens_and_one_close, btree_test_fixture) {
    using it = boost::counting_iterator<int>;
    spiderdb::btree btree{DATA_FILE};
    return seastar::do_for_each(it{0}, it{5}, [btree](int i) {
        return btree.open().handle_exception([](auto ex) {
            try {
                std::rethrow_exception(ex);
            } catch (spiderdb::spiderdb_error& err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_opened);
            }
        });
    }).then([btree] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_one_btree_multiple_concurrent_opens_and_one_close, btree_test_fixture) {
    using it = boost::counting_iterator<int>;
    spiderdb::btree btree{DATA_FILE};
    return seastar::parallel_for_each(it{0}, it{5}, [btree](int i) {
        return btree.open().handle_exception([](auto ex) {
            try {
                std::rethrow_exception(ex);
            } catch (spiderdb::spiderdb_error& err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_opened);
            }
        });
    }).then([btree] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_one_btree_one_open_and_multiple_consecutive_closes, btree_test_fixture) {
    using it = boost::counting_iterator<int>;
    spiderdb::btree btree{DATA_FILE};
    return btree.open().then([btree] {
        return seastar::do_for_each(it{0}, it{5}, [btree](int i) {
            return btree.close().handle_exception([btree](auto ex) {
                try {
                    std::rethrow_exception(ex);
                } catch (spiderdb::spiderdb_error& err) {
                    SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
                }
            });
        });
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_one_btree_one_open_and_multiple_concurrent_closes, btree_test_fixture) {
    using it = boost::counting_iterator<int>;
    spiderdb::btree btree{DATA_FILE};
    return btree.open().then([btree] {
        return seastar::parallel_for_each(it{0}, it{5}, [btree](int i) {
            return btree.close().handle_exception([btree](auto ex) {
                try {
                    std::rethrow_exception(ex);
                } catch (spiderdb::spiderdb_error& err) {
                    SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
                }
            });
        });
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_one_btree_multiple_consecutive_opens_and_closes, btree_test_fixture) {
    using it = boost::counting_iterator<int>;
    spiderdb::btree btree{DATA_FILE};
    return seastar::do_for_each(it{0}, it{5}, [btree](int i) {
        return btree.open().then([btree] {
            return btree.close().finally([btree] {});
        });
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_one_btree_multiple_concurrent_opens_and_closes, btree_test_fixture) {
    using it = boost::counting_iterator<int>;
    spiderdb::btree btree{DATA_FILE};
    return seastar::parallel_for_each(it{0}, it{5}, [btree](int i) {
        return btree.open().handle_exception([](auto ex) {
            try {
                std::rethrow_exception(ex);
            } catch (spiderdb::spiderdb_error& err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_opened);
            }
        }).then([btree] {
            return btree.close().handle_exception([btree](auto ex) {
                try {
                    std::rethrow_exception(ex);
                } catch (spiderdb::spiderdb_error& err) {
                    SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
                }
            });
        });
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_multiple_btrees_open_and_close, btree_test_fixture) {
    using it = boost::counting_iterator<int>;
    return seastar::parallel_for_each(it{0}, it{5}, [](int i) {
        spiderdb::btree btree{DATA_FILE + std::to_string(i)};
        return btree.open().then([btree] {
            return btree.close().finally([btree] {});
        });
    });
}

SPIDERDB_TEST_SUITE_END()