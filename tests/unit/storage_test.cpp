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

uint8_t get_number_of_digits(size_t num) {
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

const size_t N_RECORDS = 10000; // 100000;
const size_t SHORT_KEY_LEN = get_number_of_digits(N_RECORDS) + 1;
const size_t LONG_KEY_LEN = 1000;
const size_t SHORT_VALUE_LEN = SHORT_KEY_LEN * 5;
const size_t LONG_VALUE_LEN = LONG_KEY_LEN * 3;

struct data_generator {
    using key_t = spiderdb::string;
    using value_t = spiderdb::string;
    using item_t = std::pair<key_t, value_t>;
    using data_t = std::vector<item_t>;

public:
    void generate_sequential_data(size_t n_items, size_t from, size_t key_len, size_t value_len) {
        auto n_digits = get_number_of_digits(from + n_items);
        assert(n_digits + 1 <= key_len && n_digits + 1 <= value_len);
        data.reserve(n_items);
        for (size_t i = from; i < from + n_items; ++i) {
            spiderdb::string key{key_len - get_number_of_digits(i), '0'};
            spiderdb::string value{value_len - get_number_of_digits(i), '0'};
            key[0] = 'k';
            value[0] = 'v';
            key += spiderdb::to_string(i);
            value += spiderdb::to_string(i);
            data.push_back({std::move(key), std::move(value)});
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

private:
    data_t data;
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

SPIDERDB_TEST_SUITE(storage_test_insert)

SPIDERDB_FIXTURE_TEST_CASE(test_insert_multiple_sequential_records_consecutively, storage_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(N_RECORDS, 0, SHORT_KEY_LEN, SHORT_VALUE_LEN);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::storage storage{DATA_FILE, config};
    return storage.open().then([storage, generator] {
        return seastar::do_for_each(generator->get_data(), [storage](auto record) {
            return storage.insert(std::move(record.first), std::move(record.second));
        });
    }).finally([storage, generator] {
        return storage.close().finally([storage] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_insert_multiple_sequential_records_concurrently, storage_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(N_RECORDS, 0, SHORT_KEY_LEN, SHORT_VALUE_LEN);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::storage storage{DATA_FILE, config};
    return storage.open().then([storage, generator] {
        return seastar::parallel_for_each(generator->get_data(), [storage](auto record) {
            return storage.insert(std::move(record.first), std::move(record.second));
        });
    }).finally([storage, generator] {
        return storage.close().finally([storage] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_insert_multiple_random_records_consecutively, storage_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(N_RECORDS, 0, SHORT_KEY_LEN, SHORT_VALUE_LEN);
    generator->shuffle_data();
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::storage storage{DATA_FILE, config};
    return storage.open().then([storage, generator] {
        return seastar::do_for_each(generator->get_data(), [storage](auto record) {
            return storage.insert(std::move(record.first), std::move(record.second));
        });
    }).finally([storage, generator] {
        return storage.close().finally([storage] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_insert_multiple_random_records_concurrently, storage_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(N_RECORDS, 0, SHORT_KEY_LEN, SHORT_VALUE_LEN);
    generator->shuffle_data();
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::storage storage{DATA_FILE, config};
    return storage.open().then([storage, generator] {
        return seastar::parallel_for_each(generator->get_data(), [storage](auto record) {
            return storage.insert(std::move(record.first), std::move(record.second));
        });
    }).finally([storage, generator] {
        return storage.close().finally([storage] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_insert_multiple_records_with_long_key_and_long_value, storage_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(N_RECORDS, 0, LONG_KEY_LEN, LONG_VALUE_LEN);
    generator->shuffle_data();
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::storage storage{DATA_FILE, config};
    return storage.open().then([storage, generator] {
        return seastar::parallel_for_each(generator->get_data(), [storage](auto record) {
            return storage.insert(std::move(record.first), std::move(record.second));
        });
    }).finally([storage, generator] {
        return storage.close().finally([storage] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_insert_multiple_records_with_duplicated_key, storage_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(N_RECORDS, 0, LONG_KEY_LEN, LONG_VALUE_LEN);
    generator->shuffle_data();
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::storage storage{DATA_FILE, config};
    return storage.open().then([storage, generator] {
        return seastar::parallel_for_each(generator->get_data(), [storage](auto record) {
            return storage.insert(std::move(record.first), std::move(record.second));
        });
    }).then([storage, generator] {
        return seastar::parallel_for_each(generator->get_data(), [storage](auto record) {
            return storage.insert(std::move(record.first), std::move(record.second)).then_wrapped([](auto fut) {
                SPIDERDB_REQUIRE(fut.failed());
                try {
                    std::rethrow_exception(fut.get_exception());
                } catch (spiderdb::spiderdb_error &err) {
                    SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::key_exists);
                }
            });
        });
    }).finally([storage, generator] {
        return storage.close().finally([storage] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_insert_records_with_invalid_key_length_or_value_length, storage_test_fixture) {
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::storage storage{DATA_FILE, config};
    return storage.open().then([storage] {
        spiderdb::string key{LONG_KEY_LEN * 10, 0};
        spiderdb::string value{LONG_VALUE_LEN, 0};
        return storage.insert(std::move(key), std::move(value)).then_wrapped([](auto fut) {
            SPIDERDB_REQUIRE(fut.failed());
            try {
                std::rethrow_exception(fut.get_exception());
            } catch (spiderdb::spiderdb_error& err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::key_too_long);
            }
        });
    }).then([storage] {
        spiderdb::string key;
        spiderdb::string value{LONG_VALUE_LEN, 0};
        return storage.insert(std::move(key), std::move(value)).then_wrapped([](auto fut) {
            SPIDERDB_REQUIRE(fut.failed());
            try {
                std::rethrow_exception(fut.get_exception());
            } catch (spiderdb::spiderdb_error& err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::empty_key);
            }
        });
    }).then([storage] {
        spiderdb::string key{LONG_KEY_LEN, 0};
        spiderdb::string value;
        return storage.insert(std::move(key), std::move(value)).then_wrapped([](auto fut) {
            SPIDERDB_REQUIRE(fut.failed());
            try {
                std::rethrow_exception(fut.get_exception());
            } catch (spiderdb::spiderdb_error& err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::empty_value);
            }
        });
    }).finally([storage] {
        return storage.close().finally([storage] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_insert_before_opening, storage_test_fixture) {
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::storage storage{DATA_FILE, config};
    spiderdb::string key{LONG_KEY_LEN, 0};
    spiderdb::string value{LONG_VALUE_LEN, 0};
    return storage.insert(std::move(key), std::move(value)).then_wrapped([](auto fut) {
        SPIDERDB_REQUIRE(fut.failed());
        try {
            std::rethrow_exception(fut.get_exception());
        } catch (spiderdb::spiderdb_error &err) {
            SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
        }
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_insert_after_closing, storage_test_fixture) {
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::storage storage{DATA_FILE, config};
    return storage.open().then([storage] {
        return storage.close();
    }).then([storage] {
        spiderdb::string key{LONG_KEY_LEN, 0};
        spiderdb::string value{LONG_VALUE_LEN, 0};
        return storage.insert(std::move(key), std::move(value)).then_wrapped([](auto fut) {
            SPIDERDB_REQUIRE(fut.failed());
            try {
                std::rethrow_exception(fut.get_exception());
            } catch (spiderdb::spiderdb_error &err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
            }
        });
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_insert_after_reopening, storage_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(N_RECORDS, 0, SHORT_KEY_LEN, SHORT_VALUE_LEN);
    generator->shuffle_data();
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::storage storage{DATA_FILE, config};
    return storage.open().then([storage, generator] {
        return seastar::parallel_for_each(generator->get_data(), [storage](auto record) {
            return storage.insert(std::move(record.first), std::move(record.second));
        });
    }).finally([storage, generator] {
        return storage.close().finally([storage] {});
    }).then([storage, generator] {
        generator->clear_data();
        generator->generate_sequential_data(N_RECORDS, N_RECORDS, SHORT_KEY_LEN, SHORT_VALUE_LEN);
        generator->shuffle_data();
        return storage.open().then([storage, generator] {
            return seastar::parallel_for_each(generator->get_data(), [storage](auto record) {
                return storage.insert(std::move(record.first), std::move(record.second));
            });
        }).finally([storage, generator] {
            return storage.close().finally([storage] {});
        });
    });
}

SPIDERDB_TEST_SUITE_END()

SPIDERDB_TEST_SUITE(storage_test_select)

SPIDERDB_FIXTURE_TEST_CASE(test_select_multiple_sequential_records_consecutively, storage_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(N_RECORDS, 0, SHORT_KEY_LEN, SHORT_VALUE_LEN);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::storage storage{DATA_FILE, config};
    return storage.open().then([storage, generator] {
        return seastar::do_for_each(generator->get_data(), [storage](auto record) {
            return storage.insert(std::move(record.first), std::move(record.second));
        }).then([storage, generator] {
            return seastar::do_for_each(generator->get_data(), [storage](auto record) {
                return storage.select(std::move(record.first)).then([value{std::move(record.second)}](auto&& res) {
                    SPIDERDB_CHECK_MESSAGE(res == value, "Wrong result: Actual = {}, Expected = {}", res, value);
                });
            });
        });
    }).finally([storage, generator] {
        return storage.close().finally([storage] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_select_multiple_sequential_records_concurrently, storage_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(N_RECORDS, 0, SHORT_KEY_LEN, SHORT_VALUE_LEN);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::storage storage{DATA_FILE, config};
    return storage.open().then([storage, generator] {
        return seastar::do_for_each(generator->get_data(), [storage](auto record) {
            return storage.insert(std::move(record.first), std::move(record.second));
        }).then([storage, generator] {
            return seastar::parallel_for_each(generator->get_data(), [storage](auto record) {
                return storage.select(std::move(record.first)).then([value{std::move(record.second)}](auto&& res) {
                    SPIDERDB_CHECK_MESSAGE(res == value, "Wrong result: Actual = {}, Expected = {}", res, value);
                });
            });
        });
    }).finally([storage, generator] {
        return storage.close().finally([storage] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_select_multiple_random_records_consecutively, storage_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(N_RECORDS, 0, SHORT_KEY_LEN, SHORT_VALUE_LEN);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::storage storage{DATA_FILE, config};
    return storage.open().then([storage, generator] {
        return seastar::do_for_each(generator->get_data(), [storage](auto record) {
            return storage.insert(std::move(record.first), std::move(record.second));
        }).then([storage, generator] {
            generator->shuffle_data();
            return seastar::do_for_each(generator->get_data(), [storage](auto record) {
                return storage.select(std::move(record.first)).then([value{std::move(record.second)}](auto&& res) {
                    SPIDERDB_CHECK_MESSAGE(res == value, "Wrong result: Actual = {}, Expected = {}", res, value);
                });
            });
        });
    }).finally([storage, generator] {
        return storage.close().finally([storage] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_select_multiple_random_records_concurrently, storage_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(N_RECORDS, 0, SHORT_KEY_LEN, SHORT_VALUE_LEN);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::storage storage{DATA_FILE, config};
    return storage.open().then([storage, generator] {
        return seastar::do_for_each(generator->get_data(), [storage](auto record) {
            return storage.insert(std::move(record.first), std::move(record.second));
        }).then([storage, generator] {
            generator->shuffle_data();
            return seastar::parallel_for_each(generator->get_data(), [storage](auto record) {
                return storage.select(std::move(record.first)).then([value{std::move(record.second)}](auto&& res) {
                    SPIDERDB_CHECK_MESSAGE(res == value, "Wrong result: Actual = {}, Expected = {}", res, value);
                });
            });
        });
    }).finally([storage, generator] {
        return storage.close().finally([storage] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_select_multiple_records_with_long_key_and_long_value, storage_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(N_RECORDS, 0, LONG_KEY_LEN, LONG_VALUE_LEN);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::storage storage{DATA_FILE, config};
    return storage.open().then([storage, generator] {
        return seastar::do_for_each(generator->get_data(), [storage](auto record) {
            return storage.insert(std::move(record.first), std::move(record.second));
        }).then([storage, generator] {
            generator->shuffle_data();
            return seastar::parallel_for_each(generator->get_data(), [storage](auto record) {
                return storage.select(std::move(record.first)).then([value{std::move(record.second)}](auto&& res) {
                    SPIDERDB_CHECK_MESSAGE(res == value, "Wrong result: Actual = {}, Expected = {}", res, value);
                });
            });
        });
    }).finally([storage, generator] {
        return storage.close().finally([storage] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_select_nonexistent_records, storage_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(N_RECORDS, 0, SHORT_KEY_LEN, SHORT_VALUE_LEN);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::storage storage{DATA_FILE, config};
    return storage.open().then([storage, generator] {
        return seastar::do_for_each(generator->get_data(), [storage](auto record) {
            return storage.insert(std::move(record.first), std::move(record.second));
        }).then([storage, generator] {
            generator->clear_data();
            generator->generate_sequential_data(N_RECORDS, N_RECORDS, SHORT_KEY_LEN, SHORT_VALUE_LEN);
            generator->shuffle_data();
            return seastar::parallel_for_each(generator->get_data(), [storage](auto record) {
                return storage.select(std::move(record.first)).then_wrapped([](auto fut) {
                    SPIDERDB_REQUIRE(fut.failed());
                    try {
                        std::rethrow_exception(fut.get_exception());
                    } catch (spiderdb::spiderdb_error &err) {
                        SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::key_not_exists);
                    }
                });
            });
        });
    }).finally([storage, generator] {
        return storage.close().finally([storage] {});
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_select_before_opening, storage_test_fixture) {
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::storage storage{DATA_FILE, config};
    spiderdb::string key{LONG_KEY_LEN, 0};
    return storage.select(std::move(key)).then_wrapped([](auto fut) {
        SPIDERDB_REQUIRE(fut.failed());
        try {
            std::rethrow_exception(fut.get_exception());
        } catch (spiderdb::spiderdb_error &err) {
            SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
        }
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_select_after_closing, storage_test_fixture) {
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::storage storage{DATA_FILE, config};
    return storage.open().then([storage] {
        return storage.close();
    }).then([storage] {
        spiderdb::string key{LONG_KEY_LEN, 0};
        return storage.select(std::move(key)).then_wrapped([](auto fut) {
            SPIDERDB_REQUIRE(fut.failed());
            try {
                std::rethrow_exception(fut.get_exception());
            } catch (spiderdb::spiderdb_error &err) {
                SPIDERDB_REQUIRE(err.get_error_code() == spiderdb::error_code::file_already_closed);
            }
        });
    });
}

SPIDERDB_FIXTURE_TEST_CASE(test_find_after_reopening, storage_test_fixture) {
    auto generator = seastar::make_lw_shared<data_generator>();
    generator->generate_sequential_data(N_RECORDS, 0, SHORT_KEY_LEN, SHORT_VALUE_LEN);
    spiderdb::spiderdb_config config;
    config.log_level = seastar::log_level::debug;
    spiderdb::storage storage{DATA_FILE, config};
    return storage.open().then([storage, generator] {
        return seastar::do_for_each(generator->get_data(), [storage](auto record) {
            return storage.insert(std::move(record.first), std::move(record.second));
        });
    }).finally([storage, generator] {
        return storage.close().finally([storage] {});
    }).then([storage, generator] {
        return storage.open().then([storage, generator] {
            generator->shuffle_data();
            return seastar::parallel_for_each(generator->get_data(), [storage](auto record) {
                return storage.select(std::move(record.first)).then([value{std::move(record.second)}](auto&& res) {
                    SPIDERDB_CHECK_MESSAGE(res == value, "Wrong result: Actual = {}, Expected = {}", res, value);
                });
            });
        }).finally([storage, generator] {
            return storage.close().finally([storage] {});
        });
    });
}

SPIDERDB_TEST_SUITE_END()