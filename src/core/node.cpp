//
// Created by chungphb on 3/5/21.
//

#include <spiderdb/core/node.h>
#include <spiderdb/core/btree.h>
#include <spiderdb/util/log.h>

#include <utility>

namespace spiderdb {

seastar::future<> node_header::write(seastar::temporary_buffer<char> buffer) {
    memcpy(&_parent, buffer.begin(), sizeof(_parent));
    buffer.trim_front(sizeof(_parent));
    memcpy(&_key_count, buffer.begin(), sizeof(_key_count));
    buffer.trim_front(sizeof(_key_count));
    memcpy(&_prefix_len, buffer.begin(), sizeof(_prefix_len));
    buffer.trim_front(sizeof(_prefix_len));
    return seastar::now();
}

seastar::future<> node_header::read(seastar::temporary_buffer<char> buffer) {
    memcpy(buffer.get_write(), &_parent, sizeof(_parent));
    buffer.trim_front(sizeof(_parent));
    memcpy(buffer.get_write(), &_key_count, sizeof(_key_count));
    buffer.trim_front(sizeof(_key_count));
    memcpy(buffer.get_write(), &_prefix_len, sizeof(_prefix_len));
    buffer.trim_front(sizeof(_prefix_len));
    return seastar::now();
}

node_impl::node_impl(page page, seastar::weak_ptr<btree_impl>&& btree, seastar::weak_ptr<node_impl>&& parent) : _page{std::move(page)} {
    if (btree) {
        _btree = std::move(btree);
        _parent = std::move(parent);
        if (_parent) {
            if (_parent->_page.get_id() != _header._parent) {
                _header._parent = _parent->_page.get_id();
                _dirty = true;
            }
        }
    }

}

seastar::future<> node_impl::load() {
    if (!_btree) {
        return seastar::make_exception_future<>(std::runtime_error("Closed error"));
    }
    if (_loaded) {
        return seastar::now();
    }
    return _btree->get_file().read(_page).then([this](auto&& data) {
        auto&& is = data.get_input_stream();
        // Load prefix
        char prefix_byte_arr[_header._prefix_len];
        if (_header._prefix_len > 0) {
            is.read(prefix_byte_arr, _header._prefix_len);
            _prefix = std::move(string{prefix_byte_arr, _header._prefix_len});
        }
        // Load keys
        string current_key;
        for (uint32_t i = 0; i < _header._key_count; ++i) {
            // Load key length
            uint32_t key_len;
            char key_len_byte_arr[sizeof(key_len)];
            is.read(key_len_byte_arr, sizeof(key_len));
            memcpy(&key_len, key_len_byte_arr, sizeof(key_len));
            // Load key
            char current_key_byte_arr[_header._prefix_len + key_len];
            if (_header._prefix_len > 0) {
                memcpy(current_key_byte_arr, _prefix.c_str(), _prefix.length());
            }
            if (key_len > 0) {
                char key_byte_arr[key_len];
                is.read(key_byte_arr, key_len);
                memcpy(current_key_byte_arr + _header._prefix_len, key_byte_arr, key_len);
            }
            current_key = std::move(string{current_key_byte_arr, static_cast<size_t>(_header._prefix_len + key_len)});
            _keys.push_back(std::move(current_key));
        }
        // Load pointers
        uint32_t pointer_count = _header._key_count + (_page.get_type() == node_type::internal ? 1 : 0);
        pointer current_pointer;
        char current_pointer_byte_arr[sizeof(current_pointer)];
        for (uint32_t i = 0; i < pointer_count; ++i) {
            is.read(current_pointer_byte_arr, sizeof(current_pointer));
            memcpy(&current_pointer, current_pointer_byte_arr, sizeof(current_pointer));
            _pointers.push_back(current_pointer);
        }
        // Load high key length
        uint32_t high_key_len;
        char high_key_len_byte_arr[sizeof(high_key_len)];
        is.read(high_key_len_byte_arr, sizeof(high_key_len));
        memcpy(&high_key_len, high_key_len_byte_arr, sizeof(high_key_len));
        if (high_key_len > 0) {
            // Load high key
            char high_key_byte_arr[high_key_len];
            is.read(high_key_byte_arr, high_key_len);
            _high_key = std::move(string{high_key_byte_arr, high_key_len});
        }
        // Load siblings
        char sibling_byte_arr[sizeof(node_id)];
        is.read(sibling_byte_arr, sizeof(node_id));
        memcpy(&_prev, sibling_byte_arr, sizeof(node_id));
        is.read(sibling_byte_arr, sizeof(node_id));
        memcpy(&_next, sibling_byte_arr, sizeof(node_id));
        // Mark as loaded
        _data_len = calculate_data_length(true);
        _loaded = true;
        SPIDERDB_LOGGER_DEBUG("Node {:0>12} - Loaded", _page.get_id());
    });
}

seastar::future<> node_impl::flush() {
    if (!_btree) {
        return seastar::make_exception_future<>(std::runtime_error("Closed error"));
    }
    if (!_dirty) {
        return seastar::now();
    }
    const size_t data_len = calculate_data_length(true);
    string data{data_len, 0};
    seastar::simple_memory_output_stream os{data.str(), data.length()};
    // Flush prefix
    if (_prefix.length() > 0) {
        os.write(_prefix.c_str(), _prefix.length());
    }
    // Flush keys
    for (size_t i = 0; i < _keys.size(); i++) {
        // Flush key length
        uint32_t key_len = _keys[i].length();
        char key_len_byte_arr[sizeof(key_len)];
        memcpy(key_len_byte_arr, &key_len, sizeof(key_len));
        os.write(key_len_byte_arr, sizeof(key_len));
        if (key_len > 0) {
            // Flush key
            os.write(_keys[i].c_str() + _prefix.length(), key_len);
        }
    }
    // Flush pointers
    char current_pointer_byte_arr[sizeof(pointer)];
    for (size_t i = 0; i < _pointers.size(); i++) {
        memcpy(current_pointer_byte_arr, &_pointers[i], sizeof(pointer));
        os.write(current_pointer_byte_arr, sizeof(pointer));
    }
    // Flush high key length
    uint32_t high_key_len = _high_key.length();
    char high_key_len_byte_arr[sizeof(high_key_len)];
    memcpy(high_key_len_byte_arr, &high_key_len, sizeof(high_key_len));
    os.write(high_key_len_byte_arr, sizeof(high_key_len));
    if (high_key_len > 0) {
        // Flush high key
        os.write(_high_key.c_str(), high_key_len);
    }
    // Flush siblings
    char sibling_byte_arr[sizeof(node_id)];
    memcpy(sibling_byte_arr, &_prev, sizeof(node_id));
    os.write(sibling_byte_arr, sizeof(node_id));
    memcpy(sibling_byte_arr, &_next, sizeof(node_id));
    os.write(sibling_byte_arr, sizeof(node_id));
    return _btree->get_file().write(_page, std::move(data)).then([this] {
        // Mark as flushed
        _parent = nullptr;
        _dirty = false;
        SPIDERDB_LOGGER_DEBUG("Node {:0>12} - Flushed", _page.get_id());
    });
}

seastar::future<> node_impl::add(string&& key, data_pointer ptr) {
    if (!_btree) {
        return seastar::make_exception_future<>(std::runtime_error("Closed error"));
    }
    return seastar::with_semaphore(_lock, 1, [this, key{std::move(key)}, ptr]() mutable {
        auto id = binary_search(key, 0, _keys.size() - 1);
        switch (_page.get_type()) {
            case node_type::internal: {
                id = (id < 0) ? - (id + 1) : (id + 1);
                return get_child(id).then([key, ptr](auto child) mutable {
                    return child.add(std::move(key), ptr);
                });
            }
            case node_type::leaf: {
                if (id >= 0) {
                    return seastar::make_exception_future<>(std::runtime_error("Key existed"));
                }
                id = -id;
                _keys.insert(_keys.begin() + id, std::move(key));
                _pointers.insert(_pointers.begin() + id, pointer{.pointer = ptr});
                update_metadata();
                if (!need_split()) {
                    return seastar::now();
                }
                return split();
            }
            default: {
                return seastar::make_exception_future<>(std::runtime_error("Wrong page"));
            }
        }
    });
}

seastar::future<> node_impl::remove(string&& key) {
    if (!_btree) {
        return seastar::make_exception_future<>(std::runtime_error("Closed error"));
    }
    return seastar::with_semaphore(_lock, 1, [this, key{std::move(key)}]() mutable {
        auto id = binary_search(key, 0, _keys.size() - 1);
        switch (_page.get_type()) {
            case node_type::internal: {
                id = (id < 0) ? - (id + 1) : (id + 1);
                return get_child(id).then([key{std::move(key)}](auto child) mutable {
                    return child.remove(std::move(key));
                });
            }
            case node_type::leaf: {
                if (id < 0) {
                    return seastar::make_exception_future<>(std::runtime_error("Key not existed"));
                }
                _keys.erase(_keys.begin() + id);
                _pointers.erase(_pointers.begin() + id);
                update_metadata();
                if (!need_merge()) {
                    return seastar::now();
                }
                return merge();
            }
            default: {
                return seastar::make_exception_future<>(std::runtime_error("Wrong page"));
            }
        }
    });
}

seastar::future<data_pointer> node_impl::find(string&& key) {
    if (!_btree) {
        return seastar::make_exception_future<data_pointer>(std::runtime_error("Closed error"));
    }
    if (_next != null_node && key > _high_key) {
        return _btree->get_node(_next).then([key](auto next) mutable {
            return next.find(std::move(key));
        });
    }
    if (_prev != null_node && key < _keys[0]) {
        return _btree->get_node(_prev).then([key](auto prev) mutable {
            return prev.find(std::move(key));
        });
    }
    auto id = binary_search(key, 0, _keys.size() - 1);
    switch (_page.get_type()) {
        case node_type::internal: {
            if (_pointers.empty()) {
                return seastar::make_ready_future<data_pointer>(null_data_pointer);
            }
            id = (id < 0) ? - (id + 1) : (id + 1);
            return get_child(id).then([key{std::move(key)}](auto child) mutable {
                return child.find(std::move(key));
            });
        }
        case node_type::leaf: {
            if (id < 0) {
                return seastar::make_ready_future<data_pointer>(null_data_pointer);
            }
            return seastar::make_ready_future<data_pointer>(_pointers[id].pointer);
        }
        default: {
            return seastar::make_exception_future<data_pointer>(std::runtime_error("Wrong page"));
        }
    }
}

seastar::future<node> node_impl::get_parent() {
    if (!_btree) {
        return seastar::make_exception_future<node>(std::runtime_error("Closed error"));
    }
    if (_parent) {
        return seastar::make_ready_future<node>(_parent->shared_from_this());
    }
    if (_header._parent == null_node) {
        return seastar::make_ready_future<node>(nullptr);
    }
    if (_header._parent == root_node) {
        _parent = std::move(_btree->get_root().get_pointer());
        return seastar::make_ready_future<node>(_btree->get_root());
    }
    return _btree->get_node(_header._parent).then([this](auto parent) {
        _parent = std::move(parent.get_pointer());
        return seastar::make_ready_future<node>(parent);
    });
}

seastar::future<node> node_impl::get_child(uint32_t id) {
    if (!_btree) {
        return seastar::make_exception_future<node>(std::runtime_error("Closed error"));
    }
    if (_page.get_type() != node_type::internal || id < 0 || id >= _pointers.size()) {
        return seastar::make_exception_future<node>(std::runtime_error("Invalid access"));
    }
    return _btree->get_node(_pointers[id].child, weak_from_this());
}

int64_t node_impl::binary_search(const string& key, uint32_t low, uint32_t high) {
    while (low <= high) {
        uint32_t mid = (low + high) / 2;
        if (key < _keys[mid]) {
            high = mid - 1;
        } else if (key > _keys[mid]) {
            low = mid + 1;
        } else {
            return mid;
        }
    }
    return - (low + 1);
}

seastar::future<> node_impl::split() {
    // TODO
    return seastar::now();
}

bool node_impl::need_split() const noexcept {
    // TODO
    return false;
}

seastar::future<> node_impl::promote(string&& key, node_id left_child, node_id right_child) {
    // TODO
    return seastar::now();
}

seastar::future<> node_impl::merge() {
    // TODO
    return seastar::now();
}

bool node_impl::need_merge() const noexcept {
    // TODO
    return false;
}

seastar::future<string> node_impl::demote(node_id left_child, node_id right_child) {
    // TODO
    return seastar::make_ready_future<string>();
}

seastar::future<node_item> node_impl::first() {
    // TODO
    auto item = std::make_pair(_keys[0], _pointers[0].pointer);
    return seastar::make_ready_future<node_item>(item);
}

seastar::future<node_item> node_impl::last() {
    // TODO
    auto item = std::make_pair(_keys[0], _pointers[0].pointer);
    return seastar::make_ready_future<node_item>(item);
}

seastar::future<node_item> node_impl::next(node_item current) {
    // TODO
    auto item = std::make_pair(_keys[0], _pointers[0].pointer);
    return seastar::make_ready_future<node_item>(item);
}

seastar::future<node_item> node_impl::prev(node_item current) {
    // TODO
    auto item = std::make_pair(_keys[0], _pointers[0].pointer);
    return seastar::make_ready_future<node_item>(item);
}

void node_impl::log() const noexcept {
    // TODO
}

size_t node_impl::calculate_data_length(bool reset) noexcept {
    if (_data_len == 0 || reset) {
        size_t data_len = 0;
        data_len += _prefix.length();
        data_len += sizeof(uint32_t) * _keys.size();
        for (const auto& key : _keys) {
            data_len += key.length() - _prefix.length();
        }
        data_len += _pointers.size() * sizeof(pointer);
        data_len += sizeof(uint32_t) + _high_key.length();
        data_len += sizeof(node_id) * 2;
        _data_len = data_len;
    }
    return _data_len;
}

void node_impl::update_metadata() noexcept {
    if (_keys.size() > _btree->get_config().max_keys_on_each_node) {
        SPIDERDB_LOGGER_ERROR("Something went wrong");
        return;
    }
    _header._key_count = _keys.size();
    if (_keys.size() > 1) {
        auto old_prefix_len = _header._prefix_len;
        auto calculate_prefix_func = [](string str1, string str2) {
            size_t prefix_len = 0;
            size_t len = std::max(str1.length(), str2.length());
            for (size_t i = 0; i < len; ++i) {
                if (str1[i] == str2[i]) {
                    prefix_len++;
                } else {
                    break;
                }
            }
            if (prefix_len == 0) {
                return string{};
            }
            return string{str1.str(), prefix_len};
        };
        _prefix = std::move(calculate_prefix_func(_keys.front(), _keys.back()));
        if (_prefix.length() != old_prefix_len) {
            if (_data_len > 0) {
                _data_len += _prefix.length() - old_prefix_len;
            }
            _header._prefix_len = _prefix.length();
        }
    } else {
        _prefix = string{};
        _header._prefix_len = 0;
    }
    _dirty = true;
}

}