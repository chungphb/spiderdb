//
// Created by chungphb on 17/5/21.
//

#define SPIDERDB_USING_MASTER_TEST_SUITE
#include <spiderdb/core/storage.h>
#include <spiderdb/util/error.h>
#include <spiderdb/testing/test_case.h>
#include <boost/iterator/counting_iterator.hpp>

namespace {

const std::string DATA_FOLDER = "data";
const std::string DATA_FILE = DATA_FOLDER + "/test.dat";

struct storage_test_fixture {
    storage_test_fixture() {
        system(fmt::format("rm {}", DATA_FILE).c_str());
    }
    ~storage_test_fixture() = default;
};

}

SPIDERDB_TEST_SUITE(storage_test_open_and_close)

SPIDERDB_FIXTURE_TEST_CASE(test_one_storage_open_then_close, storage_test_fixture) {
    spiderdb::storage storage{DATA_FILE};
    return storage.open().then([storage] {
        return storage.close().finally([storage] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_one_storage_open_without_closing, storage_test_fixture) {
    spiderdb::storage storage{DATA_FILE};
    return storage.open().finally([storage] {});
}

SPIDERDB_FIXTURE_TEST_CASE(test_one_storage_close_without_opening, storage_test_fixture) {
    spiderdb::storage storage{DATA_FILE};
    return storage.close().handle_exception([storage](auto ex) {
        try {
            std::rethrow_exception(ex);
        } catch (spiderdb::spiderdb_error& err) {
            SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
        }
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_one_storage_multiple_consecutive_opens_and_one_close, storage_test_fixture) {
    using it = boost::counting_iterator<int>;
    spiderdb::storage storage{DATA_FILE};
    return seastar::do_for_each(it{0}, it{5}, [storage](int i) {
        return storage.open().handle_exception([](auto ex) {
            try {
                std::rethrow_exception(ex);
            } catch (spiderdb::spiderdb_error& err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_opened);
            }
        });
    }).then([storage] {
        return storage.close().finally([storage] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_one_storage_multiple_concurrent_opens_and_one_close, storage_test_fixture) {
    using it = boost::counting_iterator<int>;
    spiderdb::storage storage{DATA_FILE};
    return seastar::parallel_for_each(it{0}, it{5}, [storage](int i) {
        return storage.open().handle_exception([](auto ex) {
            try {
                std::rethrow_exception(ex);
            } catch (spiderdb::spiderdb_error& err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_opened);
            }
        });
    }).then([storage] {
        return storage.close().finally([storage] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_one_storage_one_open_and_multiple_consecutive_closes, storage_test_fixture) {
    using it = boost::counting_iterator<int>;
    spiderdb::storage storage{DATA_FILE};
    return storage.open().then([storage] {
        return seastar::do_for_each(it{0}, it{5}, [storage](int i) {
            return storage.close().handle_exception([storage](auto ex) {
                try {
                    std::rethrow_exception(ex);
                } catch (spiderdb::spiderdb_error& err) {
                    SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
                }
            });
        });
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_one_storage_one_open_and_multiple_concurrent_closes, storage_test_fixture) {
    using it = boost::counting_iterator<int>;
    spiderdb::storage storage{DATA_FILE};
    return storage.open().then([storage] {
        return seastar::parallel_for_each(it{0}, it{5}, [storage](int i) {
            return storage.close().handle_exception([storage](auto ex) {
                try {
                    std::rethrow_exception(ex);
                } catch (spiderdb::spiderdb_error& err) {
                    SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
                }
            });
        });
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_one_storage_multiple_consecutive_opens_and_closes, storage_test_fixture) {
    using it = boost::counting_iterator<int>;
    spiderdb::storage storage{DATA_FILE};
    return seastar::do_for_each(it{0}, it{5}, [storage](int i) {
        return storage.open().then([storage] {
            return storage.close().finally([storage] {});
        });
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_one_storage_multiple_concurrent_opens_and_closes, storage_test_fixture) {
    using it = boost::counting_iterator<int>;
    spiderdb::storage storage{DATA_FILE};
    return seastar::parallel_for_each(it{0}, it{5}, [storage](int i) {
        return storage.open().handle_exception([](auto ex) {
            try {
                std::rethrow_exception(ex);
            } catch (spiderdb::spiderdb_error& err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_opened);
            }
        }).then([storage] {
            return storage.close().handle_exception([storage](auto ex) {
                try {
                    std::rethrow_exception(ex);
                } catch (spiderdb::spiderdb_error& err) {
                    SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
                }
            });
        });
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_multiple_storages_open_and_close, storage_test_fixture) {
    using it = boost::counting_iterator<int>;
    return seastar::parallel_for_each(it{0}, it{5}, [](int i) {
        spiderdb::storage storage{DATA_FILE + std::to_string(i)};
        return storage.open().then([storage] {
            return storage.close().finally([storage] {});
        });
    });
}

SPIDERDB_TEST_SUITE_END()