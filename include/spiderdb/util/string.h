//
// Created by chungphb on 20/4/21.
//

#pragma once

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
    using char_type = char_t;

public:
    basic_string() = default;

    basic_string(const char_t* data, size_t len) {
        if (len == 0) {
            return;
        }
        if (!data) {
            throw std::invalid_argument("String: Invalid construction");
        }
        _data = static_cast<char_t*>(std::malloc(sizeof(char_t) * len));
        if (!_data) {
            throw std::bad_alloc();
        }
        memcpy(_data, data, len);
        _len = len;
    }

    basic_string(size_t len, char_t c) {
        _data = static_cast<char_t*>(std::malloc(sizeof(char_t) * len));
        if (!_data) {
            throw std::bad_alloc();
        }
        memset(_data, c, len);
        _len = len;
    }

    explicit basic_string(const std::basic_string<char_t>& str) : basic_string{str.c_str(), str.length()} {}

    explicit basic_string(std::basic_string<char_t>& str) : basic_string{str.c_str(), str.length()} {}

    explicit basic_string(const char* data) : basic_string{static_cast<const char_t*>(data), std::strlen(data)} {}

    explicit basic_string(std::basic_string_view<char_t> view) : basic_string{view.data(), view.length()} {}

    basic_string(const basic_string& str) : basic_string{str._data, str._len} {}

    basic_string<char_t>& operator=(const basic_string& str) {
        if (this != &str) {
            this->~basic_string();
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

    basic_string<char_t>& operator=(basic_string&& str) noexcept {
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
        if (!_data || id >= _len) {
            throw std::out_of_range("String: Invalid access");
        }
        return _data[id];
    }

    const char_t& operator[](size_t id) const {
        if (!_data || id >= _len) {
            throw std::out_of_range("String: Invalid access");
        }
        return _data[id];
    }

    basic_string operator+(const basic_string& str) const {
        basic_string res;
        res._data = static_cast<char_t*>(std::malloc(sizeof(char_t) * (_len + str._len)));
        if (!res._data) {
            throw std::bad_alloc();
        }
        res._len = _len + str._len;
        memcpy(res._data, _data, _len);
        memcpy(res._data + _len, str._data, str._len);
        return res;
    }

    basic_string operator+=(const basic_string& str) {
        return *this = *this + str;
    }

    bool operator==(const basic_string<char_t>& str) const noexcept {
        if (_data == str._data) {
            return true;
        }
        if (_len != str._len) {
            return false;
        }
        for (size_t id = 0; id < _len; ++id) {
            if (_data[id] != str._data[id]) {
                return false;
            }
        }
        return true;
    }

    bool operator!=(const basic_string<char_t>& str) const noexcept {
        return !operator==(str);
    }

    bool operator<(const basic_string<char_t>& str) const noexcept {
        if (_data == str._data) {
            return false;
        }
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

    bool operator>=(const basic_string<char_t>& str) const noexcept {
        return !operator<(str);
    }

    bool operator>(const basic_string<char_t>& str) const noexcept {
        if (_data == str._data) {
            return false;
        }
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

    bool operator<=(const basic_string<char_t>& str) const noexcept {
        return !operator>(str);
    }

    basic_string<char_t> clone() const {
        return {_data, _len};
    }

    seastar::simple_memory_input_stream get_input_stream() const {
        return seastar::simple_memory_input_stream{_data, _len};
    }

    explicit operator std::basic_string_view<char_t>() const noexcept {
        static_assert(noexcept(std::basic_string_view<char_t>(_data, _len)));
        return std::basic_string_view<char_t>(_data, _len);
    }

private:
    char_t* _data = nullptr;
    size_t _len = 0;
};

template <typename char_t>
std::basic_ostream<char_t>& operator<<(std::basic_ostream<char_t>& os, const basic_string<char_t>& str) {
    for (size_t id = 0; id < str.length(); ++id) {
        os << str[id];
    }
    return os;
}

namespace internal {

template <typename string_t, typename value_t>
string_t to_string_sprintf(value_t val, const char* fmt) {
    char tmp[sizeof(val) * 3 + 2];
    auto len = std::sprintf(tmp, fmt, val);
    using char_t = typename string_t::char_type;
    return string_t{reinterpret_cast<char_t*>(tmp), static_cast<size_t>(len)};
}

template <typename string_t>
string_t to_string(int val) {
    return to_string_sprintf<string_t>(val, "%d");
}

template <typename string_t>
string_t to_string(unsigned val) {
    return to_string_sprintf<string_t>(val, "%u");
}

template <typename string_t>
string_t to_string(long val) {
    return to_string_sprintf<string_t>(val, "%ld");
}

template <typename string_t>
string_t to_string(unsigned long val) {
    return to_string_sprintf<string_t>(val, "%lu");
}

template <typename string_t>
string_t to_string(long long val) {
    return to_string_sprintf<string_t>(val, "%lld");
}

template <typename string_t>
string_t to_string(unsigned long long val) {
    return to_string_sprintf<string_t>(val, "%llu");
}

template <typename string_t>
string_t to_string(float val) {
    return to_string_sprintf<string_t>(val, "%g");
}

template <typename string_t>
string_t to_string(double val) {
    return to_string_sprintf<string_t>(val, "%g");
}

}

using string = basic_string<char>;
using string_view = std::basic_string_view<char>;

template <typename string_t = string, typename value_t>
string_t to_string(value_t val) {
    return internal::to_string<string_t>(val);
}

}

namespace std {

template <typename char_t>
struct hash<spiderdb::basic_string<char_t>> {
    size_t operator()(const spiderdb::basic_string<char_t>& str) const {
        return std::hash<std::basic_string_view<char_t>>()(str);
    }
};

}