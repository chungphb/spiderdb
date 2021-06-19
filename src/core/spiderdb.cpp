//
// Created by chungphb on 6/19/21.
//

#include <spiderdb/core/spiderdb.h>
#include <spiderdb/util/hasher.h>

namespace spiderdb {

spiderdb_impl::spiderdb_impl(std::string name, spiderdb_config config) : _name{name}, _config{config} {}

seastar::future<> spiderdb_impl::open() {
    if (is_open()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::file_already_opened});
    }
    return _storage.start(_name, _config).then([this] {
        return _storage.invoke_on_all([](auto& storage) {
            return storage.open();
        });
    });
}

seastar::future<> spiderdb_impl::flush() {
    return _storage.invoke_on_all([](auto& storage) {
        return storage.flush();
    });
}

seastar::future<> spiderdb_impl::close() {
    return _storage.invoke_on_all([](auto& storage) {
        return storage.close();
    });
}

seastar::future<> spiderdb_impl::insert(string&& key, string&& value) {
    auto shard = hasher(key.clone()) % seastar::smp::count;
    return _storage.invoke_on(shard, [key{std::move(key)}, value{std::move(value)}](auto& storage) mutable {
        return storage.insert(std::move(key), std::move(value));
    });
}

seastar::future<> spiderdb_impl::update(string&& key, string&& value) {
    auto shard = hasher(key.clone()) % seastar::smp::count;
    return _storage.invoke_on(shard, [key{std::move(key)}, value{std::move(value)}](auto& storage) mutable {
        return storage.update(std::move(key), std::move(value));
    });
}

seastar::future<> spiderdb_impl::erase(string&& key) {
    auto shard = hasher(key.clone()) % seastar::smp::count;
    return _storage.invoke_on(shard, [key{std::move(key)}](auto& storage) mutable {
        return storage.erase(std::move(key));
    });
}

seastar::future<string> spiderdb_impl::select(string&& key) {
    auto shard = hasher(key.clone()) % seastar::smp::count;
    return _storage.invoke_on(shard, [key{std::move(key)}](auto& storage) mutable {
        return storage.select(std::move(key));
    });
}

bool spiderdb_impl::is_open() const noexcept {
    return _storage.local_is_initialized();
}

spiderdb::spiderdb(std::string name, spiderdb_config config) {
    _impl = seastar::make_lw_shared<spiderdb_impl>(std::move(name), config);
}

spiderdb::spiderdb(const spiderdb& other) {
    _impl = other._impl;
}

spiderdb::spiderdb(spiderdb&& other) noexcept {
    _impl = std::move(other._impl);
}

spiderdb& spiderdb::operator=(const spiderdb& other) {
    _impl = other._impl;
    return *this;
}

spiderdb& spiderdb::operator=(spiderdb&& other) noexcept {
    _impl = std::move(other._impl);
    return *this;
}

const spiderdb_config& spiderdb::get_config() const {
    if (!_impl) {
        throw spiderdb_error{error_code::invalid_database};
    }
    return _impl->_config;
}

seastar::future<> spiderdb::open() const {
    if (!_impl) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::invalid_database});
    }
    return _impl->open();
}

seastar::future<> spiderdb::close() const {
    if (!_impl) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::invalid_database});
    }
    if (!_impl->is_open()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::file_already_closed});
    }
    return _impl->close();
}

seastar::future<> spiderdb::insert(string&& key, string&& value) const {
    if (!_impl) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::invalid_database});
    }
    if (!_impl->is_open()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::file_already_closed});
    }
    return _impl->insert(std::move(key), std::move(value));
}

seastar::future<> spiderdb::update(string&& key, string&& value) const {
    if (!_impl) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::invalid_database});
    }
    if (!_impl->is_open()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::file_already_closed});
    }
    return _impl->update(std::move(key), std::move(value));
}

seastar::future<> spiderdb::erase(string&& key) const {
    if (!_impl) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::invalid_database});
    }
    if (!_impl->is_open()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::file_already_closed});
    }
    return _impl->erase(std::move(key));
}

seastar::future<string> spiderdb::select(string&& key) const {
    if (!_impl) {
        return seastar::make_exception_future<string>(spiderdb_error{error_code::invalid_database});
    }
    if (!_impl->is_open()) {
        return seastar::make_exception_future<string>(spiderdb_error{error_code::file_already_closed});
    }
    return _impl->select(std::move(key));
}

}