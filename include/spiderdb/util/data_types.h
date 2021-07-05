//
// Created by chungphb on 26/4/21.
//

#pragma once

namespace spiderdb {
namespace internal {

template <typename t, typename parameter, template <typename> class... functions>
struct strong_type : functions<strong_type<t, parameter, functions...>>... {
public:
    using underlying_type = t;

    explicit strong_type(const t& val) : _val{val} {}

    explicit strong_type(t&& val) : _val{std::move(val)} {}

    t& get() noexcept {
        return _val;
    }

    const t& get() const noexcept {
        return _val;
    }

private:
    t _val;
};

template <typename t, template<typename> class crtp_type>
struct crtp {
    t& underlying() {
        return static_cast<t&>(*this);
    }

    const t& underlying() const {
        return static_cast<const t&>(*this);
    }
};

template <typename t>
struct addable : crtp<t, addable> {
    t operator+(const t& other) const noexcept {
        return t(this->underlying().get() + other.get());
    }
};

template <typename t>
struct printable : crtp<t, printable> {
    void print(std::ostream& os) const {
        os << this->underlying().get();
    }
};

template <typename t>
struct comparable : crtp<t, comparable> {
    friend bool operator==(const comparable<t>& lhs, const t& rhs) noexcept {
        return lhs.underlying().get() == rhs.get();
    }

    friend bool operator!=(const comparable<t>& lhs, const t& rhs) noexcept {
        return lhs.underlying().get() != rhs.get();
    }

    friend bool operator>(const comparable<t>& lhs, const t& rhs) noexcept {
        return lhs.underlying().get() > rhs.get();
    }

    friend bool operator<(const comparable<t>& lhs, const t& rhs) noexcept {
        return lhs.underlying().get() < rhs.get();
    }

    friend bool operator>=(const comparable<t>& lhs, const t& rhs) noexcept {
        return lhs.underlying().get() >= rhs.get();
    }

    friend bool operator<=(const comparable<t>& lhs, const t& rhs) noexcept {
        return lhs.underlying().get() <= rhs.get();
    }
};

template <typename t>
struct hashable {
    static constexpr bool is_hashable = true;
};

}

using page_id = internal::strong_type<
        int64_t,
        struct page_id_parameter,
        internal::addable, internal::printable, internal::comparable, internal::hashable
>;
using node_id = internal::strong_type<
        int64_t,
        struct node_id_parameter,
        internal::addable, internal::printable, internal::comparable, internal::hashable
>;
using value_pointer = internal::strong_type<
        int64_t,
        struct value_pointer_parameter,
        internal::printable, internal::comparable
>;
using value_id = internal::strong_type<
        int16_t,
        struct value_pointer_parameter
>;

union node_item_pointer {
    node_id child;
    value_pointer pointer;
    using largest_type = std::conditional_t<
            (sizeof(node_id) > sizeof(value_pointer)),
            node_id::underlying_type,
            value_pointer::underlying_type
    >;
};

enum struct page_type : uint8_t {
    unused = 0,
    internal = 1,
    leaf = 2,
    data = 3,
    overflow = 4
};

inline const char* page_type_to_string(page_type type) {
    switch (type) {
        case page_type::internal: {
            return "internal";
        }
        case page_type::leaf: {
            return "leaf";
        }
        case page_type::data: {
            return "data";
        }
        case page_type::overflow: {
            return "overflow";
        }
        default: {
            return "unused";
        }
    }
}

using node_type = page_type;

const page_id null_page{-1};
const node_id null_node{-1};
const node_id root_node{0};
const value_pointer null_value_pointer{-1};

template <typename t, typename parameter, template <typename> class... functions>
std::ostream& operator<<(std::ostream& os, const internal::strong_type<t, parameter, functions...>& val) {
    val.print(os);
    return os;
}

}

namespace std {

template <typename t, typename parameter, template <typename> class... functions>
struct hash<spiderdb::internal::strong_type<t, parameter, functions...>> {
    using strong_type = spiderdb::internal::strong_type<t, parameter, functions...>;
    using check_if_hashable = std::enable_if_t<strong_type::is_hashable, void>;
    size_t operator()(const strong_type& val) const {
        return std::hash<t>()(val.get());
    }
};

}