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

struct data_generator {
    using key_t = spiderdb::string;
    using value_t = spiderdb::data_pointer;
    using item_t = std::pair<key_t, value_t>;
    using data_t = std::vector<item_t>;

public:
    void generate_sequential_data(size_t n_items, size_t from, size_t key_len) {
        assert(number_of_digits(from + n_items) + 1 <= key_len);
        data.reserve(n_items);
        for (size_t i = from; i < from + n_items; ++i) {
            spiderdb::string key{key_len - number_of_digits(i), '0'};
            key[0] = 'k';
            key += spiderdb::to_string(i);
            data.push_back({std::move(key), static_cast<spiderdb::data_pointer>(i)});
        }
    }

    void shuffle_data() {
        std::random_device rd;
        std::default_random_engine re{rd()};
        std::shuffle(data.begin(), data.end(), re);
    }

    void clear_data() {
        data.clear();
    }

    const data_t& get_data() const {
        return data;
    }

    void print_data() {
        std::stringstream str;
        for (auto it = data.begin(); it != data.end();) {
            str << "(" << it->first << ", " << it->second << ")";
            ++it;
            str << (it != data.end() ? ", " : "");
        }
        str << "\n";
        SPIDERDB_TEST_MESSAGE("{}", str.str());
    }

    static constexpr uint8_t number_of_digits(size_t num) {
        uint8_t n_digits = 0;
        if (num == 0) {
            return 1;
        }
        while (num != 0) {
            num /= 10;
            n_digits++;
        }
        return n_digits;
    }

private:
    data_t data;
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

SPIDERDB_TEST_SUITE(btree_test_add)

SPIDERDB_FIXTURE_TEST_CASE(test_add_multiple_sequential_records_consecutively, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 6);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::do_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_add_multiple_sequential_records_concurrently, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 6);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::parallel_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_add_multiple_random_records_consecutively, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 6);
    generator->shuffle_data();
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::do_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_add_multiple_random_records_concurrently, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 6);
    generator->shuffle_data();
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::parallel_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_add_multiple_records_with_long_key, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 1000);
    generator->shuffle_data();
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::parallel_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_add_multiple_records_with_duplicated_key, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 1000);
    generator->shuffle_data();
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::parallel_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        });
    }).then([btree, generator] {
        return seastar::parallel_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second).then_wrapped([](auto fut) {
                SPIDERDB_REQUIRE(fut.failed());
                try {
                    std::rethrow_exception(fut.get_exception());
                } catch (spiderdb::spiderdb_error &err) {
                    SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::key_exists);
                }
            });
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_add_records_with_invalid_key_length, btree_test_fixture) {
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree] {
        spiderdb::string key{10000, 0};
        spiderdb::data_pointer pointer{spiderdb::null_data_pointer};
        return btree.add(std::move(key), pointer).then_wrapped([](auto fut) {
            SPIDERDB_REQUIRE(fut.failed());
            try {
                std::rethrow_exception(fut.get_exception());
            } catch (spiderdb::spiderdb_error& err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::key_too_long);
            }
        });
    }).then([btree] {
        spiderdb::string key;
        spiderdb::data_pointer pointer{spiderdb::null_data_pointer};
        return btree.add(std::move(key), pointer).then_wrapped([](auto fut) {
            SPIDERDB_REQUIRE(fut.failed());
            try {
                std::rethrow_exception(fut.get_exception());
            } catch (spiderdb::spiderdb_error& err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::empty_key);
            }
        });
    }).finally([btree] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_add_before_opening, btree_test_fixture) {
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    spiderdb::string key{1000, 0};
    spiderdb::data_pointer pointer{spiderdb::null_data_pointer};
    return btree.add(std::move(key), pointer).then_wrapped([](auto fut) {
        SPIDERDB_REQUIRE(fut.failed());
        try {
            std::rethrow_exception(fut.get_exception());
        } catch (spiderdb::spiderdb_error &err) {
            SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
        }
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_add_after_closing, btree_test_fixture) {
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree] {
        return btree.close();
    }).then([btree] {
        spiderdb::string key{1000, 0};
        spiderdb::data_pointer pointer{spiderdb::null_data_pointer};
        return btree.add(std::move(key), pointer).then_wrapped([](auto fut) {
            SPIDERDB_REQUIRE(fut.failed());
            try {
                std::rethrow_exception(fut.get_exception());
            } catch (spiderdb::spiderdb_error &err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
            }
        });
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_add_after_reopening, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 6);
    generator->shuffle_data();
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::parallel_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    }).then([btree, generator] {
        generator->clear_data();
        generator->generate_sequential_data(10000, 10000, 6);
        generator->shuffle_data();
        return btree.open().then([btree, generator] {
            return seastar::parallel_for_each(generator->get_data(), [btree](auto record) {
                return btree.add(std::move(record.first), record.second);
            });
        }).finally([btree, generator] {
            return btree.close().finally([btree] {});
        });
    });
}

SPIDERDB_TEST_SUITE_END()

SPIDERDB_TEST_SUITE(btree_test_find)

SPIDERDB_FIXTURE_TEST_CASE(test_find_multiple_sequential_records_consecutively, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 6);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::do_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        }).then([btree, generator] {
            return seastar::do_for_each(generator->get_data(), [btree](auto record) {
                return btree.find(std::move(record.first)).then([data_pointer{record.second}](auto res) {
                    SPIDERDB_CHECK_MESSAGE(res == data_pointer, "Wrong result: Actual = {}, Expected = {}", res, data_pointer);
                });
            });
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_find_multiple_sequential_records_concurrently, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 6);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::do_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        }).then([btree, generator] {
            return seastar::parallel_for_each(generator->get_data(), [btree](auto record) {
                return btree.find(std::move(record.first)).then([data_pointer{record.second}](auto res) {
                    SPIDERDB_CHECK_MESSAGE(res == data_pointer, "Wrong result: Actual = {}, Expected = {}", res, data_pointer);
                });
            });
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_find_multiple_random_records_consecutively, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 6);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::do_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        }).then([btree, generator] {
            generator->shuffle_data();
            return seastar::do_for_each(generator->get_data(), [btree](auto record) {
                return btree.find(std::move(record.first)).then([data_pointer{record.second}](auto res) {
                    SPIDERDB_CHECK_MESSAGE(res == data_pointer, "Wrong result: Actual = {}, Expected = {}", res, data_pointer);
                });
            });
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_find_multiple_random_records_concurrently, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 6);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::do_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        }).then([btree, generator] {
            generator->shuffle_data();
            return seastar::parallel_for_each(generator->get_data(), [btree](auto record) {
                return btree.find(std::move(record.first)).then([data_pointer{record.second}](auto res) {
                    SPIDERDB_CHECK_MESSAGE(res == data_pointer, "Wrong result: Actual = {}, Expected = {}", res, data_pointer);
                });
            });
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_find_multiple_records_with_long_key, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 1000);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::do_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        }).then([btree, generator] {
            generator->shuffle_data();
            return seastar::parallel_for_each(generator->get_data(), [btree](auto record) {
                return btree.find(std::move(record.first)).then([data_pointer{record.second}](auto res) {
                    SPIDERDB_CHECK_MESSAGE(res == data_pointer, "Wrong result: Actual = {}, Expected = {}", res, data_pointer);
                });
            });
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_find_nonexistent_records, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 6);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::do_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        }).then([btree, generator] {
            generator->clear_data();
            generator->generate_sequential_data(10000, 10000, 6);
            generator->shuffle_data();
            return seastar::parallel_for_each(generator->get_data(), [btree](auto record) {
                return btree.find(std::move(record.first)).then([](auto res) {
                    SPIDERDB_CHECK_MESSAGE(res == spiderdb::null_data_pointer, "Wrong result");
                });
            });
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_find_before_opening, btree_test_fixture) {
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    spiderdb::string key{1000, 0};
    return btree.find(std::move(key)).then_wrapped([](auto fut) {
        SPIDERDB_REQUIRE(fut.failed());
        try {
            std::rethrow_exception(fut.get_exception());
        } catch (spiderdb::spiderdb_error &err) {
            SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
        }
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_find_after_closing, btree_test_fixture) {
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree] {
        return btree.close();
    }).then([btree] {
        spiderdb::string key{1000, 0};
        return btree.find(std::move(key)).then_wrapped([](auto fut) {
            SPIDERDB_REQUIRE(fut.failed());
            try {
                std::rethrow_exception(fut.get_exception());
            } catch (spiderdb::spiderdb_error &err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
            }
        });
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_find_after_reopening, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 6);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::do_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    }).then([btree, generator] {
        return btree.open().then([btree, generator] {
            generator->shuffle_data();
            return seastar::parallel_for_each(generator->get_data(), [btree](auto record) {
                return btree.find(std::move(record.first)).then([data_pointer{record.second}](auto res) {
                    SPIDERDB_CHECK_MESSAGE(res == data_pointer, "Wrong result: Actual = {}, Expected = {}", res, data_pointer);
                });
            });
        }).finally([btree, generator] {
            return btree.close().finally([btree] {});
        });
    });
}

SPIDERDB_TEST_SUITE_END()

SPIDERDB_TEST_SUITE(btree_test_remove)

SPIDERDB_FIXTURE_TEST_CASE(test_remove_multiple_sequential_records_consecutively, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 6);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::do_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        }).then([btree, generator] {
            generator->clear_data();
            generator->generate_sequential_data(1000, 0, 6);
            return seastar::do_for_each(generator->get_data(), [btree](auto record) {
                return btree.remove(std::move(record.first));
            }).then([btree, generator] {
                return seastar::do_for_each(generator->get_data(), [btree](auto record) {
                    return btree.find(std::move(record.first)).then([](auto res) {
                        SPIDERDB_CHECK_MESSAGE(res == spiderdb::null_data_pointer, "Wrong result");
                    });
                });
            });
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_remove_multiple_sequential_records_concurrently, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 6);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::do_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        }).then([btree, generator] {
            generator->clear_data();
            generator->generate_sequential_data(1000, 0, 6);
            return seastar::parallel_for_each(generator->get_data(), [btree](auto record) {
                return btree.remove(std::move(record.first));
            }).then([btree, generator] {
                return seastar::do_for_each(generator->get_data(), [btree](auto record) {
                    return btree.find(std::move(record.first)).then([](auto res) {
                        SPIDERDB_CHECK_MESSAGE(res == spiderdb::null_data_pointer, "Wrong result");
                    });
                });
            });
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_remove_multiple_random_records_consecutively, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 6);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::do_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        }).then([btree, generator] {
            generator->clear_data();
            generator->generate_sequential_data(1000, 0, 6);
            generator->shuffle_data();
            return seastar::do_for_each(generator->get_data(), [btree](auto record) {
                return btree.remove(std::move(record.first));
            }).then([btree, generator] {
                return seastar::do_for_each(generator->get_data(), [btree](auto record) {
                    return btree.find(std::move(record.first)).then([](auto res) {
                        SPIDERDB_CHECK_MESSAGE(res == spiderdb::null_data_pointer, "Wrong result");
                    });
                });
            });
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_remove_multiple_random_records_concurrently, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 6);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::do_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        }).then([btree, generator] {
            generator->clear_data();
            generator->generate_sequential_data(1000, 0, 6);
            generator->shuffle_data();
            return seastar::parallel_for_each(generator->get_data(), [btree](auto record) {
                return btree.remove(std::move(record.first));
            }).then([btree, generator] {
                return seastar::do_for_each(generator->get_data(), [btree](auto record) {
                    return btree.find(std::move(record.first)).then([](auto res) {
                        SPIDERDB_CHECK_MESSAGE(res == spiderdb::null_data_pointer, "Wrong result");
                    });
                });
            });
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_remove_multiple_records_with_long_key, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 1000);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::do_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        }).then([btree, generator] {
            generator->clear_data();
            generator->generate_sequential_data(1000, 0, 1000);
            generator->shuffle_data();
            return seastar::parallel_for_each(generator->get_data(), [btree](auto record) {
                return btree.remove(std::move(record.first));
            }).then([btree, generator] {
                return seastar::do_for_each(generator->get_data(), [btree](auto record) {
                    return btree.find(std::move(record.first)).then([](auto res) {
                        SPIDERDB_CHECK_MESSAGE(res == spiderdb::null_data_pointer, "Wrong result");
                    });
                });
            });
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_remove_records_multiple_times, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 6);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::do_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        }).then([btree, generator] {
            generator->clear_data();
            generator->generate_sequential_data(1000, 0, 6);
            generator->shuffle_data();
            return seastar::parallel_for_each(generator->get_data(), [btree](auto record) {
                return btree.remove(std::move(record.first));
            }).then([btree, generator] {
                return seastar::parallel_for_each(generator->get_data(), [btree](auto record) {
                    return btree.remove(std::move(record.first)).then_wrapped([](auto fut) {
                        SPIDERDB_REQUIRE(fut.failed());
                        try {
                            std::rethrow_exception(fut.get_exception());
                        } catch (spiderdb::spiderdb_error &err) {
                            SPIDERDB_REQUIRE_MESSAGE(err.get_error_code() == spiderdb::error_code::key_not_exists, "{}", err.get_error_code());
                        }
                    });
                });
            }).then([btree, generator] {
                return seastar::do_for_each(generator->get_data(), [btree](auto record) {
                    return btree.find(std::move(record.first)).then([](auto res) {
                        SPIDERDB_CHECK_MESSAGE(res == spiderdb::null_data_pointer, "Wrong result");
                    });
                });
            });
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_remove_nonexistent_records, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 6);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::do_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        }).then([btree, generator] {
            generator->clear_data();
            generator->generate_sequential_data(10000, 10000, 6);
            generator->shuffle_data();
            return seastar::parallel_for_each(generator->get_data(), [btree](auto record) {
                return btree.remove(std::move(record.first)).then_wrapped([](auto fut) {
                    SPIDERDB_REQUIRE(fut.failed());
                    try {
                        std::rethrow_exception(fut.get_exception());
                    } catch (spiderdb::spiderdb_error &err) {
                        SPIDERDB_REQUIRE_MESSAGE(err.get_error_code() == spiderdb::error_code::key_not_exists, "{}", err.get_error_code());
                    }
                });
            }).then([btree, generator] {
                return seastar::do_for_each(generator->get_data(), [btree](auto record) {
                    return btree.find(std::move(record.first)).then([](auto res) {
                        SPIDERDB_CHECK_MESSAGE(res == spiderdb::null_data_pointer, "Wrong result");
                    });
                });
            });
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_find_before_opening, btree_test_fixture) {
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    spiderdb::string key{1000, 0};
    return btree.remove(std::move(key)).then_wrapped([](auto fut) {
        SPIDERDB_REQUIRE(fut.failed());
        try {
            std::rethrow_exception(fut.get_exception());
        } catch (spiderdb::spiderdb_error &err) {
            SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
        }
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_find_after_closing, btree_test_fixture) {
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree] {
        return btree.close();
    }).then([btree] {
        spiderdb::string key{1000, 0};
        return btree.remove(std::move(key)).then_wrapped([](auto fut) {
            SPIDERDB_REQUIRE(fut.failed());
            try {
                std::rethrow_exception(fut.get_exception());
            } catch (spiderdb::spiderdb_error &err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
            }
        });
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_find_after_reopening, btree_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(10000, 0, 6);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::btree btree{DATA_FILE, config};
    return btree.open().then([btree, generator] {
        return seastar::do_for_each(generator->get_data(), [btree](auto record) {
            return btree.add(std::move(record.first), record.second);
        });
    }).finally([btree, generator] {
        return btree.close().finally([btree] {});
    }).then([btree, generator] {
        return btree.open().then([btree, generator] {
            generator->clear_data();
            generator->generate_sequential_data(1000, 0, 6);
            generator->shuffle_data();
            return seastar::parallel_for_each(generator->get_data(), [btree](auto record) {
                return btree.remove(std::move(record.first));
            }).then([btree, generator] {
                return seastar::do_for_each(generator->get_data(), [btree](auto record) {
                    return btree.find(std::move(record.first)).then([](auto res) {
                        SPIDERDB_CHECK_MESSAGE(res == spiderdb::null_data_pointer, "Wrong result");
                    });
                });
            });
        }).finally([btree, generator] {
            return btree.close().finally([btree] {});
        });
    });
}

SPIDERDB_TEST_SUITE_END()