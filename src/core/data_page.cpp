//
// Created by chungphb on 15/5/21.
//

#include <spiderdb/core/data_page.h>
#include <spiderdb/core/storage.h>
#include <spiderdb/util/log.h>

namespace spiderdb {

seastar::future<> data_page_header::write(seastar::temporary_buffer<char> buffer) {
    return node_header::write(buffer.share()).then([this, buffer{buffer.share()}]() mutable {
        buffer.trim_front(node_header::size());
        memcpy(&_value_count, buffer.begin(), sizeof(_value_count));
        buffer.trim_front(sizeof(_value_count));
        return seastar::now();
    });
}

seastar::future<> data_page_header::read(seastar::temporary_buffer<char> buffer) {
    return node_header::read(buffer.share()).then([this, buffer{buffer.share()}]() mutable {
        buffer.trim_front(node_header::size());
        memcpy(buffer.get_write(), &_value_count, sizeof(_value_count));
        buffer.trim_front(sizeof(_value_count));
        return seastar::now();
    });
}

void data_page_header::log() const noexcept {
    page_header::log();
    SPIDERDB_LOGGER_TRACE("\t{:<18}{:>20}", "Number of values: ", _value_count);
}

data_page_impl::data_page_impl(page page, seastar::weak_ptr<storage_impl>&& storage) : _page{std::move(page)} {
    if (storage) {
        _storage = std::move(storage);
        _header = seastar::dynamic_pointer_cast<data_page_header>(_page.get_header());
    }
}

seastar::future<> data_page_impl::load() {
    if (!is_valid()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::data_page_unavailable});
    }
    if (_loaded) {
        return seastar::now();
    }
    return _storage->read(_page).then([this](auto&& data) {
        auto&& is = data.get_input_stream();
        // Load values
        for (uint32_t i = 0; i < _header->_value_count; ++i) {
            // Load value length
            uint32_t value_len;
            char value_len_byte_arr[sizeof(value_len)];
            is.read(value_len_byte_arr, sizeof(value_len));
            memcpy(&value_len, value_len_byte_arr, sizeof(value_len));
            // Load value
            char value_byte_arr[value_len];
            if (value_len > 0) {
                is.read(value_byte_arr, value_len);
            }
            string value{value_byte_arr, value_len};
            _values.push_back(std::move(value));
        }
        // Mark as loaded
        _data_len = data.length();
        _loaded = true;
    });
}

seastar::future<> data_page_impl::flush() {
    if (!is_valid()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::data_page_unavailable});
    }
    if (_page.get_type() != node_type::data) {
        return seastar::now();
    }
    if (!_dirty) {
        return seastar::now();
    }
    string data{_data_len, 0};
    seastar::simple_memory_output_stream os{data.str(), data.length()};
    // Flush values
    for (size_t i = 0; i < _values.size(); ++i) {
        // Flush value length
        uint32_t value_len = _values[i].length();
        char value_len_byte_arr[sizeof(value_len)];
        memcpy(value_len_byte_arr, &value_len, sizeof(value_len));
        os.write(value_len_byte_arr, sizeof(value_len));
        if (value_len > 0) {
            // Flush value
            os.write(_values[i].c_str(), value_len);
        }
    }
    return _storage->write(_page, std::move(data)).then([this] {
        // Mark as flushed
        _dirty = false;
    });
}

seastar::future<value_id> data_page_impl::add(string&& value) {
    if (!is_valid()) {
        return seastar::make_exception_future<value_id>(spiderdb_error{error_code::data_page_unavailable});
    }
    return seastar::with_lock(_rwlock.for_write(), [this, value{std::move(value)}]() mutable {
        auto value_len = value.length();
        _values.push_back(std::move(value));
        _data_len += value_len + sizeof(uint32_t);
        ++_header->_value_count;
        _dirty = true;
        return seastar::make_ready_future<value_id>(_values.size() - 1);
    }).then([this](auto result) {
        return cache(shared_from_this()).then([result] {
            return seastar::make_ready_future<value_id>(result);
        });
    });
}

seastar::future<> data_page_impl::update(value_id id, string&& value) {
    if (!is_valid()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::data_page_unavailable});
    }
    if (id < 0 || id >= _values.size() || _values[id].empty()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::value_not_exists});
    }
    return seastar::with_lock(_rwlock.for_write(), [this, id, value{std::move(value)}]() mutable {
        auto old_value_len = _values[id].length();
        auto new_value_len = value.length();
        _values[id] = std::move(value);
        _data_len += new_value_len - old_value_len;
        _dirty = true;
        return seastar::now();
    }).then([this] {
        return cache(shared_from_this());
    });
}

seastar::future<> data_page_impl::remove(value_id id) {
    if (!is_valid()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::data_page_unavailable});
    }
    if (id < 0 || id >= _values.size() || _values[id].empty()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::value_not_exists});
    }
    return seastar::with_lock(_rwlock.for_write(), [this, id] {
        auto value_len = _values[id].length();
        _values[id] = string{};
        _data_len -= value_len;
        --_header->_value_count;
        _dirty = true;
        return seastar::now();
    }).then([this] {
        if (_header->_value_count == 0 && _values.size() == _storage->_config.max_empty_values_on_each_page) {
            return clean();
        }
        return cache(shared_from_this());
    });
}

seastar::future<string> data_page_impl::find(value_id id) {
    if (!is_valid()) {
        return seastar::make_exception_future<string>(spiderdb_error{error_code::data_page_unavailable});
    }
    if (id < 0 || id >= _values.size() || _values[id].empty()) {
        return seastar::make_exception_future<string>(spiderdb_error{error_code::value_not_exists});
    }
    return seastar::with_lock(_rwlock.for_read(), [this, id] {
        return seastar::make_ready_future<string>(_values[id]);
    }).then([this](auto result) {
        return cache(shared_from_this()).then([result] {
            return seastar::make_ready_future<string>(result);
        });
    });
}

void data_page_impl::log() const {
    _page.log();
    // FIXME
}

seastar::future<> data_page_impl::cache(data_page data_page) {
    // FIXME
    return seastar::now();
}

seastar::future<> data_page_impl::clean() {
    if (!is_valid()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::node_unavailable});
    }
    _page.set_type(node_type::unused);
    _data_len = 0;
    _header->_value_count = 0;
    SPIDERDB_LOGGER_DEBUG("Page {:0>12} - Cleaned", _page.get_id());
    return _storage->unlink_pages_from(_page);
}

bool data_page_impl::is_valid() const noexcept {
    return (bool)_storage && (bool)_header;
}

data_page::data_page(page page, seastar::weak_ptr<storage_impl>&& storage) {
    _impl = seastar::make_lw_shared<data_page_impl>(std::move(page), std::move(storage));
}

data_page::data_page(seastar::lw_shared_ptr<data_page_impl> impl) {
    _impl = std::move(impl);
}

data_page::data_page(const data_page& other_data_page) {
    _impl = other_data_page._impl;
}

data_page::data_page(data_page&& other_data_page) noexcept {
    _impl = std::move(other_data_page._impl);
}

data_page& data_page::operator=(const data_page& other_data_page) {
    _impl = other_data_page._impl;
    return *this;
}

data_page& data_page::operator=(data_page&& other_data_page) noexcept {
    _impl = std::move(other_data_page._impl);
    return *this;
}

data_page::operator bool() const noexcept {
    return (bool)_impl;
}

bool data_page::operator!() const noexcept {
    return !(bool)_impl;
}

page_id data_page::get_id() const {
    if (!_impl) {
        throw spiderdb_error{error_code::data_page_unavailable};
    }
    return _impl->_page.get_id();
}

seastar::weak_ptr<data_page_impl> data_page::get_pointer() const {
    if (!_impl) {
        throw spiderdb_error{error_code::data_page_unavailable};
    }
    return _impl->weak_from_this();
}

page data_page::get_page() const {
    if (!_impl) {
        throw spiderdb_error{error_code::data_page_unavailable};
    }
    return _impl->_page;
}

const std::vector<string>& data_page::get_value_list() const {
    if (!_impl) {
        throw spiderdb_error{error_code::data_page_unavailable};
    }
    return _impl->_values;
}

size_t data_page::get_data_length() const {
    if (!_impl) {
        throw spiderdb_error{error_code::data_page_unavailable};
    }
    return _impl->_data_len;
}

void data_page::mark_dirty() const {
    if (!_impl) {
        throw spiderdb_error{error_code::data_page_unavailable};
    }
    _impl->_dirty = true;
}

seastar::future<> data_page::load() const {
    if (!_impl) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::data_page_unavailable});
    }
    return _impl->load();
}

seastar::future<> data_page::flush() const {
    if (!_impl) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::data_page_unavailable});
    }
    return _impl->flush();
}

seastar::future<value_id> data_page::add(string&& value) const {
    if (!_impl) {
        return seastar::make_exception_future<value_id>(spiderdb_error{error_code::data_page_unavailable});
    }
    return _impl->add(std::move(value));
}

seastar::future<> data_page::update(value_id id, string&& value) const {
    if (!_impl) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::data_page_unavailable});
    }
    return _impl->update(id, std::move(value));
}

seastar::future<> data_page::remove(value_id id) const {
    if (!_impl) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::data_page_unavailable});
    }
    return _impl->remove(id);
}

seastar::future<string> data_page::find(value_id id) const {
    if (!_impl) {
        return seastar::make_exception_future<string>(spiderdb_error{error_code::data_page_unavailable});
    }
    return _impl->find(id);
}

void data_page::log() const {
    if (!_impl) {
        throw spiderdb_error{error_code::data_page_unavailable};
    }
    _impl->log();
}

}