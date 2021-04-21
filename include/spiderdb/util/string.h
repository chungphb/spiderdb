//
// Created by chungphb on 20/4/21.
//

#pragma once

#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/simple-stream.hh>
#include <type_traits>

namespace spiderdb {

template <typename char_t>
struct basic_string {

static_assert(
    std::is_same_v<char_t, char> ||
    std::is_same_v<char_t, unsigned char> ||
    std::is_same_v<char_t, signed char> ||
    std::is_same_v<char_t, uint8_t> ||
    std::is_same_v<char_t, int8_t>,
    "Not supported"
);

public:
    basic_string() = default;

    basic_string(char_t* data, size_t len) {
        if (len == 0) {
            return;
        }
        if (!data) {
            throw std::runtime_error("Non-zero length empty string");
        }
        _data = static_cast<char_t*>(std::malloc(sizeof(char_t) * len));
        if (!_data) {
            throw std::bad_alloc();
        }
        std::memcpy(_data, data, len);
        _len = len;
    }

    basic_string(const char_t* data, size_t len) {
        if (len == 0) {
            return;
        }
        if (!data) {
            throw std::runtime_error("Non-zero length empty string");
        }
        _data = static_cast<char_t*>(std::malloc(sizeof(char_t) * len));
        if (!_data) {
            throw std::bad_alloc();
        }
        std::memcpy(_data, data, len);
        _len = len;
    }

    basic_string(std::basic_string<char_t> str) : basic_string(str.c_str(), str.length()) {}

    basic_string(seastar::temporary_buffer<char_t>&& buffer) : basic_string(buffer.get(), buffer.size()) {}

    basic_string(const basic_string& str) : basic_string(str._data, str._len) {}

    basic_string<char_t>& operator=(const basic_string& str) {
        if (this != &str) {
            new (this) basic_string(str);
        }
        return *this;
    }

    basic_string(basic_string&& str) noexcept {
        _data = str._data;
        _len = str._len;
        str._data = nullptr;
        str._len = 0;
    }

    basic_string<char_t>& operator=(basic_string&& str) {
        if (this != &str) {
            this->~basic_string();
            new (this) basic_string(std::move(str));
        }
        return *this;
    }

    ~basic_string() {
        std::free(_data);
        _data = nullptr;
        _len = 0;
    }

    char_t* str() noexcept {
        return _data;
    }

    const char_t* c_str() const noexcept {
        return _data;
    }

    size_t size() const noexcept {
        return _len;
    }

    size_t length() const noexcept {
        return _len;
    }

    bool empty() const noexcept {
        return _len == 0;
    }

    char_t& operator[](size_t id) {
        if (!_data) {
            throw std::runtime_error("Invalid access");
        }
        if (id < 0 || id >= _len) {
            throw std::runtime_error("Invalid access");
        }
        return _data[id];
    }

    const char_t& operator[](size_t id) const {
        if (!_data) {
            throw std::runtime_error("Invalid access");
        }
        if (id < 0 || id >= _len) {
            throw std::runtime_error("Invalid access");
        }
        return _data[id];
    }

    const bool operator==(const basic_string<char_t>& str) const {
        if (_len != str._len) {
            return false;
        }
        if (_data == str._data) {
            return true;
        }
        for (size_t id = 0; id < _len; ++id) {
            if (_data[id] != str._data[id]) {
                return false;
            }
        }
        return true;
    }

    const bool operator!=(const basic_string<char_t>& str) const {
        return !operator==(str);
    }

    const bool operator<(const basic_string<char_t>& str) const {
        const auto min_len = std::min(_len, str._len);
        for (size_t id = 0; id < min_len; ++id) {
            if (_data[id] < str._data[id]) {
                return true;
            }
            if (_data[id] > str._data[id]) {
                return false;
            }
        }
        return _len < str._len;
    }

    const bool operator>=(const basic_string<char_t>& str) const {
        return !operator<(str);
    }

    const bool operator>(const basic_string<char_t>& str) const {
        const auto min_len = std::min(_len, str._len);
        for (size_t id = 0; id < min_len; ++id) {
            if (_data[id] > str._data[id]) {
                return true;
            }
            if (_data[id] < str._data[id]) {
                return false;
            }
        }
        return _len > str._len;
    }

    const bool operator<=(const basic_string<char_t>& str) const {
        return !operator>(str);
    }

    seastar::simple_memory_input_stream get_input_stream() const {
        return seastar::simple_memory_input_stream(_data, _len);
    }

    friend std::hash<basic_string>;

    friend std::ostream& operator<<(std::ostream& os, basic_string str) {
        for (size_t id = 0; id < str.length(); id++) {
            os << str[id];
        }
        return os;
    }

private:
    char_t* _data = nullptr;
    size_t _len = 0;
};

using string = basic_string<char>;

}

template <typename char_t>
struct std::hash<spiderdb::basic_string<char_t>> {
    size_t operator()(const spiderdb::basic_string<char_t>& str) const {
        return std::hash<char_t*>{}(str._data) * std::hash<int>{}(static_cast<int>(str._len));
    }
};