//
// Created by chungphb on 22/4/21.
//

#define SPIDERDB_USING_MASTER_TEST_SUITE
#include <spiderdb/util/cache.h>
#include <spiderdb/testing/test_case.h>
#include <type_traits>
#include <unordered_set>
#include <set>

namespace {

template <typename key_t, typename value_t>
using item_pair_t = std::pair<key_t, value_t>;

template <typename key_t, typename value_t>
using item_list_t = std::list<item_pair_t<key_t, value_t>>;

struct hash {
    template <typename key_t, typename value_t>
    std::size_t operator()(const item_pair_t<key_t, value_t>& item) const {
        return std::hash<key_t>()(item.first) ^ std::hash<value_t>()(item.second);
    }
};

template <typename key_t, typename value_t>
item_list_t<key_t, value_t> generate_consecutive_data(size_t n_items, key_t from) {
    static_assert(std::is_same_v<key_t, int> && std::is_constructible_v<value_t, key_t>);
    item_list_t<key_t, value_t> list;
    for (size_t i = 0; i < n_items; ++i) {
        key_t key = from + i;
        value_t value{key};
        list.push_back({key, value});
    }
    return list;
}

template <typename key_t, typename value_t>
item_list_t<key_t, value_t> generate_random_data(size_t n_items, key_t min, key_t max) {
    static_assert(std::is_same_v<key_t, int> && std::is_constructible_v<value_t, key_t>);
    assert(min <= max);
    item_list_t<key_t, value_t> list;
    for (size_t i = 0; i < n_items; ++i) {
        key_t key = std::rand() % (max - min) + min;
        value_t value{key};
        list.push_back({key, value});
    }
    return list;
}

template <typename key_t, typename value_t>
item_list_t<key_t, value_t> reverse(const item_list_t<key_t, value_t>& list) {
    static_assert(std::is_same_v<key_t, int> && std::is_constructible_v<value_t, key_t>);
    auto res = list;
    res.reverse();
    return res;
}

template <typename key_t, typename value_t>
item_list_t<key_t, value_t> remove_duplication(const item_list_t<key_t, value_t>& list) {
    static_assert(std::is_same_v<key_t, int> && std::is_constructible_v<value_t, key_t>);
    auto res = list;
    std::unordered_set<item_pair_t<key_t, value_t>, hash> set;
    res.template remove_if([&set](const item_pair_t<key_t, value_t>& item) {
        if (set.find(item) == set.end()) {
            set.insert(item);
            return false;
        }
        return true;
    });
    return res;
}

template <typename key_t, typename value_t>
item_list_t<key_t, value_t> get_first_n_items(const item_list_t<key_t, value_t>& list, size_t n) {
    static_assert(std::is_same_v<key_t, int> && std::is_constructible_v<value_t, key_t>);
    assert(n <= list.size());
    item_list_t<key_t, value_t> res;
    std::copy_n(list.begin(), n, std::back_inserter(res));
    return res;
}

template <typename key_t, typename value_t>
bool is_equal(const item_list_t<key_t, value_t>& list1, const item_list_t<key_t, value_t>& list2) {
    static_assert(std::is_same_v<key_t, int> && std::is_constructible_v<value_t, key_t>);
    if (list1.size() != list2.size()) {
        return false;
    }
    for (auto it1 = list1.begin(), it2 = list2.begin(); it1 != list1.end(); ++it1, ++it2) {
        if (*it1 != *it2) {
            return false;
        }
    }
    return true;
}

template <typename key_t, typename value_t>
bool has_duplication(const item_list_t<key_t, value_t>& list) {
    static_assert(std::is_same_v<key_t, int> && std::is_constructible_v<value_t, key_t>);
    std::unordered_set<item_pair_t<key_t, value_t>, hash> set(list.begin(), list.end());
    return set.size() == list.size();
}

template <typename container_t>
void print(const char* name, const container_t& list) {
    std::string str = name;
    str += ":\n";
    for (auto it = list.begin(); it != list.end();) {
        str += std::to_string(it->first);
        ++it;
        str += (it != list.end() ? ", " : "");
    }
    str += "\n";
    SPIDERDB_TEST_MESSAGE("{}", str);
}

namespace internal {

struct sample {
public:
    sample(int v) : var{v} {}
    bool operator==(const sample& s) const {
        return var == s.var;
    }
    bool operator!=(const sample& s) const {
        return var != s.var;
    }
    friend struct std::hash<sample>;
public:
    int var = 0;
};

}
}

namespace std {

template <>
struct hash<internal::sample> {
    size_t operator()(const internal::sample& s) const {
        return hash<int>()(s.var);
    }
};

}

SPIDERDB_TEST_SUITE(cache_test)

SPIDERDB_TEST_CASE(test_put_consecutive_data_with_number_of_unique_items_less_than_or_qual_to_cache_capacity) {
    // Create cache
    const size_t capacity = 256;
    const auto cache_history = seastar::make_lw_shared<std::list<std::pair<int, int>>>();
    auto evictor = [cache_history](const std::pair<int, int>& evicted_item) -> seastar::future<> {
        cache_history->push_front(evicted_item);
        return seastar::now();
    };
    const auto cache = seastar::make_lw_shared<spiderdb::cache<int, int>>(capacity, std::move(evictor));

    // Generate access pattern
    const auto n_unique_items = 64;
    const auto access_pattern = seastar::make_lw_shared(generate_consecutive_data<int, int>(n_unique_items, 0));
    print("Access pattern", *access_pattern);

    // Put items into cache
    return seastar::do_for_each(*access_pattern, [cache](const auto& item) {
        const auto& key = item.first;
        const auto& value = item.second;
        return cache->put(key, value);
    }).then([cache, access_pattern, cache_history] {
        // Check result
        const auto& expected_items = reverse(*access_pattern);
        print("Cache items (actual)", cache->get_all_items());
        print("Cache items (expected)", expected_items);
        SPIDERDB_REQUIRE((is_equal<int, int>(cache->get_all_items(), expected_items)));
        SPIDERDB_REQUIRE(cache_history->empty());
    });
}

SPIDERDB_TEST_CASE(test_put_consecutive_data_with_number_of_unique_items_greater_than_cache_capacity) {
    // Create cache
    const size_t capacity = 256;
    const auto cache_history = seastar::make_lw_shared<std::list<std::pair<int, int>>>();
    auto evictor = [cache_history](const std::pair<int, int>& evicted_item) -> seastar::future<> {
        cache_history->push_front(evicted_item);
        return seastar::now();
    };
    const auto cache = seastar::make_lw_shared<spiderdb::cache<int, int>>(capacity, std::move(evictor));

    // Generate access pattern
    const auto n_unique_items = 512;
    const auto access_pattern = seastar::make_lw_shared(generate_consecutive_data<int, int>(n_unique_items, 0));
    print("Access pattern", *access_pattern);

    // Put items into cache
    return seastar::do_for_each(*access_pattern, [cache](const auto& item) {
        const auto& key = item.first;
        const auto& value = item.second;
        return cache->put(key, value);
    }).then([cache, access_pattern, cache_history, capacity, n_unique_items] {
        // Check result
        const auto& expected_items = get_first_n_items(reverse(*access_pattern), capacity);
        print("Cache items (actual)", cache->get_all_items());
        print("Cache items (expected)", expected_items);
        SPIDERDB_REQUIRE((is_equal<int, int>(cache->get_all_items(), expected_items)));
        const auto& expected_cache_history = reverse(get_first_n_items(*access_pattern, n_unique_items - capacity));
        SPIDERDB_REQUIRE((is_equal<int, int>(*cache_history, expected_cache_history)));
    });
}

SPIDERDB_TEST_CASE(test_put_random_data_with_number_of_unique_items_less_than_or_qual_to_cache_capacity) {
    // Create cache
    const size_t capacity = 256;
    const auto cache_history = seastar::make_lw_shared<std::list<std::pair<int, int>>>();
    auto evictor = [cache_history](const std::pair<int, int>& evicted_item) -> seastar::future<> {
        cache_history->push_front(evicted_item);
        return seastar::now();
    };
    const auto cache = seastar::make_lw_shared<spiderdb::cache<int, int>>(capacity, std::move(evictor));

    // Generate access pattern
    const auto n_unique_items = 64; // Just an upper bound of the actual number of unique items
    const auto access_pattern = seastar::make_lw_shared(generate_random_data<int, int>(256, 0, n_unique_items));
    print("Access pattern", *access_pattern);

    // Put items into cache
    return seastar::do_for_each(*access_pattern, [cache](const auto& item) {
        const auto& key = item.first;
        const auto& value = item.second;
        return cache->put(key, value);
    }).then([cache, access_pattern, cache_history] {
        // Check result
        const auto& expected_items = remove_duplication(reverse(*access_pattern));
        print("Cache items (actual)", cache->get_all_items());
        print("Cache items (expected)", expected_items);
        SPIDERDB_REQUIRE((is_equal<int, int>(cache->get_all_items(), expected_items)));
        SPIDERDB_REQUIRE(cache_history->empty());
    });
}

SPIDERDB_TEST_CASE(test_put_random_data_with_number_of_unique_items_greater_than_cache_capacity) {
    // Create cache
    const size_t capacity = 256;
    const auto cache_history = seastar::make_lw_shared<std::list<std::pair<int, int>>>();
    auto evictor = [cache_history](const std::pair<int, int>& evicted_item) -> seastar::future<> {
        cache_history->push_front(evicted_item);
        return seastar::now();
    };
    const auto cache = seastar::make_lw_shared<spiderdb::cache<int, int>>(capacity, std::move(evictor));

    // Generate access pattern
    auto max_item = 256;
    auto access_pattern = seastar::make_lw_shared<item_list_t<int, int>>();
    size_t n_unique_items = 0;
    while (n_unique_items <= capacity) {
        max_item += 256;
        *access_pattern = generate_random_data<int, int>(512, 0, max_item);
        n_unique_items = remove_duplication(*access_pattern).size();
    }
    print("Access pattern", *access_pattern);

    // Put items into cache
    return seastar::do_for_each(*access_pattern, [cache](const auto& item) {
        const auto& key = item.first;
        const auto& value = item.second;
        return cache->put(key, value);
    }).then([cache, access_pattern, cache_history, capacity] {
        // Check result
        const auto& expected_items = get_first_n_items(remove_duplication(reverse(*access_pattern)), capacity);
        print("Cache items (actual)", cache->get_all_items());
        print("Cache items (expected)", expected_items);
        SPIDERDB_REQUIRE((is_equal<int, int>(cache->get_all_items(), expected_items)));
        SPIDERDB_REQUIRE(!cache_history->empty());
    });
}

SPIDERDB_TEST_CASE(test_get) {
    // Create cache
    const size_t capacity = 256;
    const auto cache_history = seastar::make_lw_shared<std::list<std::pair<int, int>>>();
    auto evictor = [cache_history](const std::pair<int, int>& evicted_item) -> seastar::future<> {
        cache_history->push_front(evicted_item);
        return seastar::now();
    };
    const auto cache = seastar::make_lw_shared<spiderdb::cache<int, int>>(capacity, std::move(evictor));

    // Generate cache data
    const auto cache_size = 256;
    assert(cache_size <= capacity);
    const auto cache_data = seastar::make_lw_shared(generate_consecutive_data<int, int>(cache_size, 0));
    print("Cache data", *cache_data);
    *cache_data = reverse(*cache_data);

    // Put items into cache
    return seastar::do_for_each(*cache_data, [cache](const auto& item) {
        const auto& key = item.first;
        const auto& value = item.second;
        return cache->put(key, value);
    }).then([cache, cache_data, max_item{cache_size}] {
        // Generate access pattern
        const auto access_pattern = seastar::make_lw_shared(generate_random_data<int, int>(512, 0, max_item));
        print("Access pattern", *access_pattern);

        // Get items from cache
        return seastar::do_for_each(*access_pattern, [cache](const auto& item) {
            const auto& key = item.first;
            const auto& value = item.second;
            return cache->get(key).then_wrapped([cache, &key, &value](auto fut) {
                SPIDERDB_REQUIRE_MESSAGE(!fut.failed(), "Key {} not found", key);
                SPIDERDB_REQUIRE_MESSAGE(fut.get0() == value, "Key {} has a different value", key);
            });
        }).then([cache, access_pattern] {
            // Check result
            const auto n_unique_items = remove_duplication(*access_pattern).size();
            const auto& actual_items = get_first_n_items(cache->get_all_items(), n_unique_items);
            const auto& expected_items = remove_duplication(reverse(*access_pattern));
            print("Cache items (actual)", actual_items);
            print("Cache items (expected)", expected_items);
            SPIDERDB_REQUIRE((is_equal<int, int>(actual_items, expected_items)));
        });
    });
}

SPIDERDB_TEST_CASE(test_clear) {
    // Create cache
    const size_t capacity = 256;
    const auto cache_history = seastar::make_lw_shared<std::list<std::pair<int, int>>>();
    auto evictor = [cache_history](const std::pair<int, int>& evicted_item) -> seastar::future<> {
        cache_history->push_front(evicted_item);
        return seastar::now();
    };
    const auto cache = seastar::make_lw_shared<spiderdb::cache<int, int>>(capacity, std::move(evictor));

    // Generate cache data
    const auto cache_size = 256;
    assert(cache_size <= capacity);
    const auto cache_data = seastar::make_lw_shared(generate_consecutive_data<int, int>(cache_size, 0));
    print("Cache data", *cache_data);
    *cache_data = reverse(*cache_data);

    // Put items into cache
    return seastar::do_for_each(*cache_data, [cache](const auto& item) {
        const auto& key = item.first;
        const auto& value = item.second;
        return cache->put(key, value);
    }).then([cache, cache_data, cache_history] {
        // Clear cache
        return cache->clear().then([cache, cache_data, cache_history] {
            // Check result
            const auto& expected_cache_history = reverse(*cache_data);
            print("Cache history (actual)", *cache_history);
            print("Cache history (expected)", expected_cache_history);
            SPIDERDB_REQUIRE((is_equal<int, int>(*cache_history, expected_cache_history)));
        });
    });
}

SPIDERDB_TEST_CASE(test_cache_user_defined_types) {
    using namespace internal;

    // Create cache
    const size_t capacity = 256;
    const auto cache_history = seastar::make_lw_shared<std::list<std::pair<int, sample>>>();
    auto evictor = [cache_history](const std::pair<int, sample>& evicted_item) -> seastar::future<> {
        cache_history->push_front(evicted_item);
        return seastar::now();
    };
    const auto cache = seastar::make_lw_shared<spiderdb::cache<int, sample>>(capacity, std::move(evictor));

    // Generate access pattern
    const auto n_unique_items = 256;
    const auto access_pattern = seastar::make_lw_shared(generate_consecutive_data<int, sample>(n_unique_items, 0));
    print("Access pattern", *access_pattern);
    *access_pattern = reverse(*access_pattern);

    // Put items into cache
    return seastar::do_for_each(*access_pattern, [cache](const auto& item) {
        const auto& key = item.first;
        const auto& value = item.second;
        return cache->put(key, value);
    }).then([cache, access_pattern] {
        // Check result
        const auto& expected_items = reverse(*access_pattern);
        print("Cache items after put (actual)", cache->get_all_items());
        print("Cache items after put (expected)", expected_items);
        SPIDERDB_REQUIRE((is_equal<int, sample>(cache->get_all_items(), expected_items)));
    }).then([cache, access_pattern] {
        // Get items from cache
        return seastar::do_for_each(*access_pattern, [cache](const auto& item) {
            const auto& key = item.first;
            const auto& value = item.second;
            return cache->get(key).then_wrapped([cache, &key, &value](auto fut) {
                SPIDERDB_REQUIRE_MESSAGE(!fut.failed(), "Key {} not found", key);
                SPIDERDB_REQUIRE_MESSAGE(fut.get0() == value, "Key {} has a different value", key);
            });
        }).then([cache, access_pattern] {
            // Check result
            const auto& expected_items = reverse(*access_pattern);
            print("Cache items after get (actual)", cache->get_all_items());
            print("Cache items after get (expected)", expected_items);
            SPIDERDB_REQUIRE((is_equal<int, sample>(cache->get_all_items(), expected_items)));
        });
    }).then([cache, access_pattern, cache_history] {
        // Clear cache
        return cache->clear().then([cache, access_pattern, cache_history] {
            // Check result
            const auto& expected_cache_history = reverse(*access_pattern);
            print("Cache history (actual)", *cache_history);
            print("Cache history (expected)", expected_cache_history);
            SPIDERDB_REQUIRE((is_equal<int, sample>(*cache_history, expected_cache_history)));
        });
    });
}

SPIDERDB_TEST_CASE(test_empty_cache) {
    // Create cache
    const size_t capacity = 0;
    const auto cache_history = seastar::make_lw_shared<std::list<std::pair<int, int>>>();
    auto evictor = [cache_history](const std::pair<int, int>& evicted_item) -> seastar::future<> {
        cache_history->push_front(evicted_item);
        return seastar::now();
    };
    const auto cache = seastar::make_lw_shared<spiderdb::cache<int, int>>(capacity, std::move(evictor));

    // Generate access pattern
    const auto n_unique_items = 256;
    const auto access_pattern = seastar::make_lw_shared(generate_consecutive_data<int, int>(n_unique_items, 0));
    *access_pattern = reverse(*access_pattern);

    // Put items into cache
    return seastar::do_for_each(*access_pattern, [cache](const auto& item) {
        const auto& key = item.first;
        const auto& value = item.second;
        return cache->put(key, value);
    }).then([cache] {
        // Check result
        SPIDERDB_REQUIRE(cache->empty());
    }).then([cache, access_pattern] {
        // Get items from cache
        return seastar::do_for_each(*access_pattern, [cache](const auto& item) {
            const auto& key = item.first;
            return cache->get(key).then_wrapped([&key](auto fut) {
                SPIDERDB_REQUIRE_MESSAGE(fut.failed(), "Key {} found", key);
                try {
                    std::rethrow_exception(fut.get_exception());
                } catch (spiderdb::cache_error& err) {
                    SPIDERDB_REQUIRE(strcmp(err.what(), "Item not exists") == 0);
                }
            });
        }).then([cache] {
            // Check result
            SPIDERDB_REQUIRE(cache->empty());
        });
    }).then([cache, access_pattern, cache_history] {
        // Clear cache
        return cache->clear().then([cache] {
            // Check result
            SPIDERDB_REQUIRE(cache->empty());
        });
    });
}

SPIDERDB_TEST_CASE(test_concurrent_access) {
    // Create cache
    const size_t capacity = 256;
    const auto cache_history = seastar::make_lw_shared<std::list<std::pair<int, int>>>();
    auto evictor = [cache_history](const std::pair<int, int>& evicted_item) -> seastar::future<> {
        cache_history->push_front(evicted_item);
        return seastar::now();
    };
    const auto cache = seastar::make_lw_shared<spiderdb::cache<int, int>>(capacity, std::move(evictor));

    // Generate access pattern
    const auto n_unique_items = 256;
    const auto access_pattern = seastar::make_lw_shared(generate_random_data<int, int>(512, 0, n_unique_items));
    *access_pattern = reverse(*access_pattern);

    // Check result
    const auto put_items = seastar::make_lw_shared<item_list_t<int, int>>();
    const auto get_items = seastar::make_lw_shared<item_list_t<int, int>>();
    return seastar::parallel_for_each(*access_pattern, [cache, put_items, get_items](const auto& item) {
        const auto& key = item.first;
        const auto& value = item.second;
        if (std::rand() % 2 == 0) {
            return cache->put(key, value).then([put_items, &item] {
                put_items->push_back(item);
            });
        } else {
            return cache->get(key).then_wrapped([get_items, &item](auto fut) {
                if (!fut.failed()) {
                    get_items->push_back(item);
                } else {
                    try {
                        std::rethrow_exception(fut.get_exception());
                    } catch (spiderdb::cache_error& err) {
                        SPIDERDB_REQUIRE(strcmp(err.what(), "Item not exists") == 0);
                    }
                }
            });
        }
    }).then([cache, put_items, get_items] {
        std::set<item_pair_t<int, int>> put_set{put_items->begin(), put_items->end()};
        std::set<item_pair_t<int, int>> get_set{get_items->begin(), get_items->end()};
        print("Put set", put_set);
        print("Get set", get_set);
        std::list<item_pair_t<int, int>> res;
        std::set_difference(get_set.begin(), get_set.end(), put_set.begin(), put_set.end(), std::back_inserter(res));
        SPIDERDB_REQUIRE(res.empty());
    });
}

SPIDERDB_TEST_SUITE_END()
