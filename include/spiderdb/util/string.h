//
// Created by chungphb on 20/4/21.
//

#pragma once

#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/simple-stream.hh>

namespace spiderdb {

template <typename char_t>
struct basic_string {
public:
    basic_string() = default;

    basic_string(char* data, size_t len) {
        _len = len;
        _data = static_cast<char*>(std::malloc(sizeof(char) * _len));
        std::memcpy(_data, data, _len);
    }

    basic_string(std::basic_string<char_t> str) {
        _len = str.length();
        _data = static_cast<char*>(std::malloc(sizeof(char) * _len));
        std::memcpy(_data, const_cast<char*>(str.c_str()), _len);
    }

    basic_string(seastar::temporary_buffer<char_t> buffer) {
        _len = buffer.size();
        _data = static_cast<char*>(std::malloc(sizeof(char) * _len));
        std::memcpy(_data, buffer.get_write(), _len);
    }

    basic_string(const basic_string& str) = default;

    basic_string<char_t>& operator=(const basic_string& str) = default;

    basic_string(basic_string&& str) noexcept = default;

    basic_string<char_t>& operator=(basic_string&& str) noexcept = default;

    ~basic_string() {
        std::free(_data);
        _data = nullptr;
        _len = 0;
    }

    char* str() {
        return _data;
    }

    const char* c_str() const {
        return _data;
    }

    size_t length() {
        return _len;
    }

    char_t& operator[](size_t id) {
        if (id < 0 || id >= _len) {
            throw std::runtime_error("Invalid access");
        } else {
            return _data[id];
        }
    }

    const char_t& operator[](size_t id) const {
        if (id < 0 || id >= _len) {
            throw std::runtime_error("Invalid access");
        } else {
            return _data[id];
        }
    }

    const bool operator==(const basic_string<char_t>& str) {
        if (_len != str._len) {
            return false;
        } else {
            for (size_t id = 0; id < _len; id++) {
                if (_data[id] != str._data[id]) {
                    return false;
                }
            }
            return true;
        }
    }

    const bool operator!=(const basic_string<char_t>& str) {
        return !operator==(str);
    }

    const bool operator<(const basic_string<char_t>& str) {
        const auto min_len = std::min(_len, str._len);
        for (size_t id = 0; id < min_len; id++) {
            if (_data[id] < str._data[id]) {
                return true;
            }
            if (_data[id] > str._data[id]) {
                return false;
            }
        }
        return _len < str._len;
    }

    const bool operator>=(const basic_string<char_t>& str) {
        return !operator<(str);
    }

    const bool operator>(const basic_string<char_t>& str) {
        const auto min_len = std::min(_len, str._len);
        for (size_t id = 0; id < min_len; id++) {
            if (_data[id] > str._data[id]) {
                return true;
            }
            if (_data[id] < str._data[id]) {
                return false;
            }
        }
        return _len > str._len;
    }

    const bool operator<=(const basic_string<char_t>& str) {
        return !operator>(str);
    }

    seastar::simple_memory_input_stream get_input_stream() {
        return seastar::simple_memory_input_stream(_data, _len);
    }

    friend std::hash<basic_string<char_t>>;

private:
    char* _data;
    size_t _len;
};

using string = basic_string<char>;

}

template <typename char_t>
struct std::hash<spiderdb::basic_string<char_t>> {
    size_t operator()(const spiderdb::basic_string<char>& str) const {
        return std::hash<char*>{}(str._data) * std::hash<int>{}(static_cast<int>(str._len));
    }
};