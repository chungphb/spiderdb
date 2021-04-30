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

namespace {

struct test_write_fixture {
    test_write_fixture() {
        system(fmt::format("rm {}", DATA_FILE).c_str());
    }
    ~test_write_fixture() = default;
};

spiderdb::string generate_data(char c = '0', size_t len = 1 << 16, const char* prefix = "") {
    assert(strlen(prefix) <= len);
    spiderdb::string res{len, c};
    memcpy(res.str(), prefix, strlen(prefix));
    return res;
}

}

SPIDERDB_TEST_SUITE(file_test_write)

SPIDERDB_FIXTURE_TEST_CASE(test_write_a_regular_string, test_write_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::trace;
    spiderdb::file file{DATA_FILE, config};
    return file.open().then([file] {
        spiderdb::string str = std::move(generate_data('0'));
        return file.write(str);
    }).finally([file] {
        return file.close().finally([file] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_write_an_empty_string, test_write_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::trace;
    spiderdb::file file{DATA_FILE, config};
    return file.open().then([file] {
        spiderdb::string str;
        return file.write(str);
    }).finally([file] {
        return file.close().finally([file] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_write_before_opening, test_write_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::trace;
    spiderdb::file file{DATA_FILE, config};
    spiderdb::string str = std::move(generate_data('0'));
    return file.write(str).finally([file] {});
}

SPIDERDB_FIXTURE_TEST_CASE(test_write_after_closing, test_write_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::trace;
    spiderdb::file file{DATA_FILE, config};
    return file.open().then([file] {
        return file.close();
    }).then([file] {
        spiderdb::string str = std::move(generate_data('0'));
        return file.write(str).finally([file] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_write_multiple_strings_consecutively, test_write_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::trace;
    spiderdb::file file{DATA_FILE, config};
    return file.open().then([file] {
        using it = boost::counting_iterator<int>;
        return seastar::do_for_each(it{0}, it{5}, [file](int i) {
            spiderdb::string str = std::move(generate_data(static_cast<char>('0' + i)));
            return file.write(str).then([i] {
                SPIDERDB_TEST_MESSAGE("Wrote string {} successfully", i);
            });
        });
    }).finally([file] {
        return file.close().finally([file] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_write_multiple_strings_concurrently, test_write_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::trace;
    spiderdb::file file{DATA_FILE, config};
    return file.open().then([file] {
        using it = boost::counting_iterator<int>;
        return seastar::parallel_for_each(it{0}, it{5}, [file](int i) {
            spiderdb::string str = std::move(generate_data(static_cast<char>('0' + i)));
            return file.write(str).then([i] {
                SPIDERDB_TEST_MESSAGE("Wrote string {} successfully", i);
            });
        });
    }).finally([file] {
        return file.close().finally([file] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_write_after_reopening, test_write_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::trace;
    spiderdb::file file{DATA_FILE, config};
    return file.open().then([file] {
        spiderdb::string str = std::move(generate_data(static_cast<char>('0')));
        return file.write(str);
    }).finally([file] {
        return file.close().finally([file] {});
    }).then([file] {
        return file.open().then([file] {
            spiderdb::string str = std::move(generate_data(static_cast<char>('1')));
            return file.write(str);
        }).finally([file] {
            return file.close().finally([file] {});
        });
    });
}

SPIDERDB_TEST_SUITE_END()
