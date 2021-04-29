//
// Created by chungphb on 29/4/21.
//

#define SPIDERDB_USING_MASTER_TEST_SUITE
#include <spiderdb/core/file.h>
#include <spiderdb/testing/test_case.h>
#include <boost/iterator/counting_iterator.hpp>

namespace {

const std::string DATA_FOLDER = "data";
const std::string DATA_FILE = DATA_FOLDER + "/test.dat";

}

SPIDERDB_TEST_SUITE(file_test_open_and_close)

SPIDERDB_TEST_CASE(test_one_file_open_then_close) {
    spiderdb::file file{DATA_FILE};
    return file.open().then([file] {
        return file.close().finally([file] {});
    });
}

SPIDERDB_TEST_CASE(test_one_file_open_without_closing) {
    spiderdb::file file{DATA_FILE};
    return file.open().finally([file] {});
}

SPIDERDB_TEST_CASE(test_one_file_close_without_opening) {
    spiderdb::file file{DATA_FILE};
    return file.close().finally([file] {});
}

SPIDERDB_TEST_CASE(test_one_file_multiple_consecutive_opens_and_one_close) {
    using it = boost::counting_iterator<int>;
    spiderdb::file file{DATA_FILE};
    return seastar::do_for_each(it{0}, it{5}, [file](int i) {
        return file.open();
    }).then([file] {
        return file.close().finally([file] {});
    });
}

SPIDERDB_TEST_CASE(test_one_file_multiple_concurrent_opens_and_one_close) {
    using it = boost::counting_iterator<int>;
    spiderdb::file file{DATA_FILE};
    return seastar::parallel_for_each(it{0}, it{5}, [file](int i) {
        return file.open();
    }).then([file] {
        return file.close().finally([file] {});
    });
}

SPIDERDB_TEST_CASE(test_one_file_one_open_and_multiple_consecutive_closes) {
    using it = boost::counting_iterator<int>;
    spiderdb::file file{DATA_FILE};
    return file.open().then([file] {
        return seastar::do_for_each(it{0}, it{5}, [file](int i) {
            return file.close().finally([file] {});
        });
    });
}

SPIDERDB_TEST_CASE(test_one_file_one_open_and_multiple_concurrent_closes) {
    using it = boost::counting_iterator<int>;
    spiderdb::file file{DATA_FILE};
    return file.open().then([file] {
        return seastar::parallel_for_each(it{0}, it{5}, [file](int i) {
            return file.close().finally([file] {});
        });
    });
}

SPIDERDB_TEST_CASE(test_one_file_multiple_consecutive_opens_and_closes) {
    using it = boost::counting_iterator<int>;
    spiderdb::file file{DATA_FILE};
    return seastar::do_for_each(it{0}, it{5}, [file](int i) {
        return file.open().then([file] {
            return file.close().finally([file] {});
        });
    });
}

SPIDERDB_TEST_CASE(test_one_file_multiple_concurrent_opens_and_closes) {
    using it = boost::counting_iterator<int>;
    spiderdb::file file{DATA_FILE};
    return seastar::parallel_for_each(it{0}, it{5}, [file](int i) {
        return file.open().then([file] {
            return file.close().finally([file] {});
        });
    });
}

SPIDERDB_TEST_CASE(test_multiple_files_open_and_close) {
    using it = boost::counting_iterator<int>;
    return seastar::parallel_for_each(it{0}, it{5}, [](int i) {
        spiderdb::file file{DATA_FILE + std::to_string(i)};
        return file.open().then([file] {
            return file.close().finally([file] {});
        });
    });
}

SPIDERDB_TEST_SUITE_END()
