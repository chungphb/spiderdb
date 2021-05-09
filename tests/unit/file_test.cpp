//
// Created by chungphb on 29/4/21.
//

#define SPIDERDB_USING_MASTER_TEST_SUITE
#include <spiderdb/core/file.h>
#include <spiderdb/util/error.h>
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
    return file.close().handle_exception([file](auto ex) {
        try {
            std::rethrow_exception(ex);
        } catch (spiderdb::spiderdb_error& err) {
            SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
        }
    });
}

SPIDERDB_TEST_CASE(test_one_file_multiple_consecutive_opens_and_one_close) {
    using it = boost::counting_iterator<int>;
    spiderdb::file file{DATA_FILE};
    return seastar::do_for_each(it{0}, it{5}, [file](int i) {
        return file.open().handle_exception([](auto ex) {
            try {
                std::rethrow_exception(ex);
            } catch (spiderdb::spiderdb_error& err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_opened);
            }
        });
    }).then([file] {
        return file.close().finally([file] {});
    });
}

SPIDERDB_TEST_CASE(test_one_file_multiple_concurrent_opens_and_one_close) {
    using it = boost::counting_iterator<int>;
    spiderdb::file file{DATA_FILE};
    return seastar::parallel_for_each(it{0}, it{5}, [file](int i) {
        return file.open().handle_exception([](auto ex) {
            try {
                std::rethrow_exception(ex);
            } catch (spiderdb::spiderdb_error& err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_opened);
            }
        });
    }).then([file] {
        return file.close().finally([file] {});
    });
}

SPIDERDB_TEST_CASE(test_one_file_one_open_and_multiple_consecutive_closes) {
    using it = boost::counting_iterator<int>;
    spiderdb::file file{DATA_FILE};
    return file.open().then([file] {
        return seastar::do_for_each(it{0}, it{5}, [file](int i) {
            return file.close().handle_exception([file](auto ex) {
                try {
                    std::rethrow_exception(ex);
                } catch (spiderdb::spiderdb_error& err) {
                    SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
                }
            });
        });
    });
}

SPIDERDB_TEST_CASE(test_one_file_one_open_and_multiple_concurrent_closes) {
    using it = boost::counting_iterator<int>;
    spiderdb::file file{DATA_FILE};
    return file.open().then([file] {
        return seastar::parallel_for_each(it{0}, it{5}, [file](int i) {
            return file.close().handle_exception([file](auto ex) {
                try {
                    std::rethrow_exception(ex);
                } catch (spiderdb::spiderdb_error& err) {
                    SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
                }
            });
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
        return file.open().handle_exception([](auto ex) {
            try {
                std::rethrow_exception(ex);
            } catch (spiderdb::spiderdb_error& err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_opened);
            }
        }).then([file] {
            return file.close().handle_exception([file](auto ex) {
                try {
                    std::rethrow_exception(ex);
                } catch (spiderdb::spiderdb_error& err) {
                    SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
                }
            });
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

struct file_test_fixture {
    file_test_fixture() {
        system(fmt::format("rm {}", DATA_FILE).c_str());
    }
    ~file_test_fixture() = default;
};

spiderdb::string generate_data(char c = '0', size_t len = 1 << 16, const char* prefix = "") {
    assert(strlen(prefix) <= len);
    spiderdb::string res{len, c};
    memcpy(res.str(), prefix, strlen(prefix));
    return res;
}

}

SPIDERDB_TEST_SUITE(file_test_write)

SPIDERDB_FIXTURE_TEST_CASE(test_write_a_regular_string, file_test_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::file file{DATA_FILE, config};
    return file.open().then([file] {
        spiderdb::string str = std::move(generate_data('0'));
        return file.write(str).then([](auto page_id) {
            SPIDERDB_CHECK_MESSAGE(page_id == 0, "Wrong page");
        });
    }).finally([file] {
        return file.close().finally([file] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_write_an_empty_string, file_test_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::file file{DATA_FILE, config};
    return file.open().then([file] {
        spiderdb::string str;
        return file.write(str).then([](auto page_id) {
            SPIDERDB_REQUIRE(false);
        }).handle_exception([](auto ex) {
            try {
                std::rethrow_exception(ex);
            } catch (std::invalid_argument& err) {
                SPIDERDB_REQUIRE(strcmp(err.what(), "Write empty data") == 0);
            }
        });
    }).finally([file] {
        return file.close().finally([file] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_write_before_opening, file_test_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::file file{DATA_FILE, config};
    spiderdb::string str = std::move(generate_data('0'));
    return file.write(str).then([](auto page_id) {
        SPIDERDB_REQUIRE(false);
    }).handle_exception([](auto ex) {
        try {
            std::rethrow_exception(ex);
        } catch (spiderdb::spiderdb_error& err) {
            SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
        }
    }).finally([file] {});
}

SPIDERDB_FIXTURE_TEST_CASE(test_write_after_closing, file_test_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::file file{DATA_FILE, config};
    return file.open().then([file] {
        return file.close();
    }).then([file] {
        spiderdb::string str = std::move(generate_data('0'));
        return file.write(str).then([](auto page_id) {
            SPIDERDB_REQUIRE(false);
        }).handle_exception([](auto ex) {
            try {
                std::rethrow_exception(ex);
            } catch (spiderdb::spiderdb_error& err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
            }
        }).finally([file] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_write_multiple_strings_consecutively, file_test_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::file file{DATA_FILE, config};
    return file.open().then([file, work_size{config.page_size - config.page_header_size}] {
        using it = boost::counting_iterator<int>;
        return seastar::do_for_each(it{0}, it{5}, [file, work_size](int i) {
            spiderdb::string str = std::move(generate_data(static_cast<char>('0' + i)));
            return file.write(str).then([i, work_size, str_len(str.length())](auto page_id) {
                SPIDERDB_CHECK_MESSAGE(page_id == i * ((str_len - 1) / work_size + 1), "Wrong page");
                SPIDERDB_TEST_MESSAGE("String {} written", i);
            });
        });
    }).finally([file] {
        return file.close().finally([file] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_write_multiple_strings_concurrently, file_test_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::file file{DATA_FILE, config};
    return file.open().then([file, work_size{config.page_size - config.page_header_size}] {
        using it = boost::counting_iterator<int>;
        return seastar::parallel_for_each(it{0}, it{5}, [file, work_size](int i) {
            spiderdb::string str = std::move(generate_data(static_cast<char>('0' + i)));
            return file.write(str).then([i, work_size, str_len(str.length())](auto page_id) {
                SPIDERDB_CHECK_MESSAGE(page_id < 5 * ((str_len - 1) / work_size + 1), "Wrong page");
                SPIDERDB_TEST_MESSAGE("String {} written", i);
            });
        });
    }).finally([file] {
        return file.close().finally([file] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_write_after_reopening, file_test_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::file file{DATA_FILE, config};
    return file.open().then([file] {
        spiderdb::string str = std::move(generate_data(static_cast<char>('0')));
        return file.write(str).then([file](auto page_id) {
            SPIDERDB_CHECK_MESSAGE(page_id == 0, "Wrong page");
        });
    }).finally([file] {
        return file.close().finally([file] {});
    }).then([file, work_size{config.page_size - config.page_header_size}] {
        return file.open().then([file, work_size] {
            spiderdb::string str = std::move(generate_data(static_cast<char>('1')));
            return file.write(str).then([file, work_size, str_len{str.length()}](auto page_id) {
                SPIDERDB_CHECK_MESSAGE(page_id == (str_len - 1) / work_size + 1, "Wrong page");
            });
        }).finally([file] {
            return file.close().finally([file] {});
        });
    });
}

SPIDERDB_TEST_SUITE_END()

SPIDERDB_TEST_SUITE(file_test_read)

SPIDERDB_FIXTURE_TEST_CASE(test_read_a_regular_page, file_test_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::file file{DATA_FILE, config};
    return file.open().then([file] {
        spiderdb::string str = std::move(generate_data('0'));
        return file.write(str).then([file, str](auto page_id) {
            return file.read(page_id).then([str](auto res) {
                SPIDERDB_CHECK_MESSAGE(res == str, "Wrong result");
            });
        });
    }).finally([file] {
        return file.close().finally([file] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_read_invalid_pages, file_test_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::file file{DATA_FILE, config};
    return file.open().then([file] {
        return file.read(spiderdb::null_page).then_wrapped([](auto fut) {
            SPIDERDB_REQUIRE(fut.failed());
            try {
                std::rethrow_exception(fut.get_exception());
            } catch (spiderdb::spiderdb_error& err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::invalid_page);
            }
        }).then([file] {
            return file.read(spiderdb::page_id{INT64_MAX}).then_wrapped([](auto fut) {
                SPIDERDB_REQUIRE(fut.failed());
                try {
                    std::rethrow_exception(fut.get_exception());
                } catch (spiderdb::spiderdb_error& err) {
                    SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::invalid_page);
                }
            });
        });
    }).finally([file] {
        return file.close().finally([file] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_read_before_opening, file_test_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::file file{DATA_FILE, config};
    return file.read(spiderdb::page_id{0}).then_wrapped([](auto fut) {
        SPIDERDB_REQUIRE(fut.failed());
        try {
            std::rethrow_exception(fut.get_exception());
        } catch (spiderdb::spiderdb_error& err) {
            SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
        }
    }).finally([file] {});
}

SPIDERDB_FIXTURE_TEST_CASE(test_read_after_closing, file_test_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::file file{DATA_FILE, config};
    return file.open().then([file] {
        return file.close();
    }).then([file] {
        return file.read(spiderdb::page_id{0}).then_wrapped([](auto fut) {
            SPIDERDB_REQUIRE(fut.failed());
            try {
                std::rethrow_exception(fut.get_exception());
            } catch (spiderdb::spiderdb_error& err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
            }
        }).finally([file] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_read_a_page_multiple_times, file_test_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::file file{DATA_FILE, config};
    return file.open().then([file] {
        spiderdb::string str = std::move(generate_data('0'));
        return file.write(str).then([file, str](auto page_id) {
            using it = boost::counting_iterator<int>;
            return seastar::parallel_for_each(it{0}, it{5}, [file, str, page_id](int) {
                return file.read(page_id).then([str](auto res) {
                    SPIDERDB_CHECK_MESSAGE(res == str, "Wrong result");
                });
            });
        });
    }).finally([file] {
        return file.close().finally([file] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_read_after_reopening, file_test_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::file file{DATA_FILE, config};
    using data_t = std::pair<spiderdb::string, spiderdb::page_id>;
    auto data = seastar::make_lw_shared<data_t>(std::move(generate_data('0')), spiderdb::null_page);
    return file.open().then([file, data] {
        return file.write(data->first).then([file, data](auto page_id) {
            data->second = page_id;
            return file.read(page_id).then([data](auto res) {
                SPIDERDB_CHECK_MESSAGE(res == data->first, "Wrong result");
            });
        });
    }).finally([file] {
        return file.close().finally([file] {});
    }).then([file, data] {
        return file.open().then([file, data] {
            return file.read(data->second).then([data](auto res) {
                SPIDERDB_CHECK_MESSAGE(res == data->first, "Wrong result");
            });
        }).finally([file] {
            return file.close().finally([file] {});
        });
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_read_multiple_pages_consecutively, file_test_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::file file{DATA_FILE, config};
    return file.open().then([file] {
        using data_t = std::vector<std::pair<spiderdb::string, spiderdb::page_id>>;
        auto data = seastar::make_lw_shared<data_t>();
        for (int i = 0; i < 5; i++) {
            data->push_back(std::make_pair(std::move(generate_data(static_cast<char>('0' + i))), spiderdb::null_page));
        }
        return seastar::parallel_for_each(*data, [file](auto& item) {
            return file.write(item.first).then([&item](auto page_id) {
                item.second = page_id;
                SPIDERDB_TEST_MESSAGE("String written to page {}", page_id);
            });
        }).then([file, data] {
            return seastar::do_for_each(*data, [file](auto& item) {
                return file.read(item.second).then([&item](auto res) {
                    SPIDERDB_CHECK_MESSAGE(res == item.first, "Wrong result");
                    SPIDERDB_TEST_MESSAGE("String read from page {}", item.second);
                });
            });
        }).finally([data] {});
    }).finally([file] {
        return file.close().finally([file] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_read_multiple_pages_concurrently, file_test_fixture) {
    spiderdb::file_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::file file{DATA_FILE, config};
    return file.open().then([file] {
        using data_t = std::vector<std::pair<spiderdb::string, spiderdb::page_id>>;
        auto data = seastar::make_lw_shared<data_t>();
        for (int i = 0; i < 5; i++) {
            data->push_back(std::make_pair(std::move(generate_data(static_cast<char>('0' + i))), spiderdb::null_page));
        }
        return seastar::parallel_for_each(*data, [file](auto& item) {
            return file.write(item.first).then([&item](auto page_id) {
                item.second = page_id;
                SPIDERDB_TEST_MESSAGE("String written to page {}", page_id);
            });
        }).then([file, data] {
            return seastar::parallel_for_each(*data, [file](auto& item) {
                return file.read(item.second).then([&item](auto res) {
                    SPIDERDB_CHECK_MESSAGE(res == item.first, "Wrong result");
                    SPIDERDB_TEST_MESSAGE("String read from page {}", item.second);
                });
            });
        }).finally([data] {});
    }).finally([file] {
        return file.close().finally([file] {});
    });
}

SPIDERDB_TEST_SUITE_END()
