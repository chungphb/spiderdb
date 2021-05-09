//
// Created by chungphb on 22/4/21.
//

#pragma once

#include <spiderdb/util/error.h>
#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/shared_mutex.hh>
#include <list>
#include <unordered_map>

namespace spiderdb {

template <typename key_t, typename value_t>
struct cache {
    static_assert(!std::is_reference_v<key_t> && !std::is_reference_v<value_t>, "Not supported");
    using key_value_pair_t = std::pair<key_t, value_t>;
    using list_iterator_t = typename std::list<key_value_pair_t>::iterator;
    using evictor_t = std::function<seastar::future<>(const key_value_pair_t&)>;

public:
    cache() = delete;

    cache(size_t capacity, evictor_t evictor) : _capacity{capacity}, _evictor{std::move(evictor)} {}

    ~cache() = default;

    seastar::future<> put(key_t key, value_t value) {
        return seastar::with_lock(_lock, [this, key{std::forward<key_t>(key)}, value{std::forward<value_t>(value)}] {
            _items.push_front({key, value});
            auto iterator_it = _iterators.find(key);
            if (iterator_it != _iterators.end()) {
                _items.erase(iterator_it->second);
                _iterators.erase(iterator_it);
            }
            _iterators[key] = _items.begin();
            return seastar::do_until([this] {
                return _items.size() <= _capacity;
            }, [this] {
                return _evictor(_items.back()).then([this] {
                    _iterators.erase(_items.back().first);
                    _items.pop_back();
                });
            });
        });
    }

    seastar::future<value_t> get(key_t key) {
        return seastar::with_lock(_lock, [this, key{std::forward<key_t>(key)}] {
            auto iterator_it = _iterators.find(key);
            if (iterator_it == _iterators.end()) {
                return seastar::make_exception_future<value_t>(cache_error{"Item not exists"});
            }
            _items.splice(_items.begin(), _items, iterator_it->second);
            return seastar::make_ready_future<value_t>(iterator_it->second->second);
        });
    }

    const std::list<key_value_pair_t>& get_all_items() const noexcept {
        return _items;
    }

    seastar::future<> clear() noexcept {
        return seastar::with_lock(_lock, [this] {
            return seastar::do_for_each(_items.rbegin(), _items.rend(), [this](const auto& item) {
                return _evictor(item);
            }).then([this] {
                _items.clear();
                _iterators.clear();
            });
        });
    }

    size_t size() const noexcept {
        return _items.size();
    }

    size_t capacity() const noexcept {
        return _capacity;
    }

    bool empty() const noexcept {
        return _items.empty();
    }

private:
    const size_t _capacity;
    std::list<key_value_pair_t> _items;
    std::unordered_map<key_t, list_iterator_t> _iterators;
    evictor_t _evictor;
    seastar::shared_mutex _lock;
};

}