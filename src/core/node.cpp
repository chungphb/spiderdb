//
// Created by chungphb on 3/5/21.
//

#include <spiderdb/core/node.h>
#include <spiderdb/core/btree.h>
#include <spiderdb/util/log.h>

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
                _keys.insert(_keys.begin() + id, key);
                _pointers.insert(_pointers.begin() + id, pointer{.pointer = ptr});
                update_metadata();
                _data_len += key.length() + sizeof(uint32_t) + sizeof(pointer);
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
                return get_child(id).then([key](auto child) mutable {
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
                _data_len -= key.length() + sizeof(uint32_t) + sizeof(pointer);
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
    if (_header._parent == _btree->get_root().get_id()) {
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
    if (!_btree) {
        return seastar::make_exception_future<>(std::runtime_error("Closed error"));
    }
    // Prepare data
    auto midpoint = _header._key_count / 2;
    std::vector<string> left_keys, right_keys;
    std::vector<pointer> left_pointers, right_pointers;
    switch (_page.get_type()) {
        case node_type::internal: {
            left_keys.insert(left_keys.end(), _keys.begin(), _keys.begin() + midpoint);
            right_keys.insert(right_keys.end(), _keys.begin() + midpoint + 1, _keys.end());
            left_pointers.insert(left_pointers.end(), _pointers.begin(), _pointers.begin() + midpoint + 1);
            right_pointers.insert(right_pointers.end(), _pointers.begin() + midpoint + 1, _pointers.end());
            break;
        }
        case node_type::leaf: {
            left_keys.insert(left_keys.end(), _keys.begin(), _keys.begin() + midpoint);
            right_keys.insert(right_keys.end(), _keys.begin() + midpoint, _keys.end());
            left_pointers.insert(left_pointers.end(), _pointers.begin(), _pointers.begin() + midpoint);
            right_pointers.insert(right_pointers.end(), _pointers.begin() + midpoint, _pointers.end());
            break;
        }
        default: {
            return seastar::make_exception_future<>(std::runtime_error("Wrong page"));
        }
    }
    // Split
    if (_page.get_id() == _btree->get_root().get_id()) {
        return seastar::when_all_succeed(create_node(std::move(left_keys), std::move(left_pointers)),
                                         create_node(std::move(right_keys), std::move(right_pointers)))
                .then([this, midpoint](auto children) {
                    node left, right;
                    std::tie(left, right) = std::move(children);
                    left.set_high_key(_keys[midpoint].clone());
                    right.set_high_key(_high_key.clone());
                    return link_siblings(left, right).then([this, midpoint, left, right] {
                        SPIDERDB_LOGGER_DEBUG("Node {:0>12} - Split to {} + {}", _page.get_id(), left.get_id(), right.get_id());
                        _page.set_type(node_type::internal);
                        std::vector<string> keys{_keys[midpoint].clone()};
                        std::vector<pointer> pointers{pointer{.child = left.get_id()}, pointer{.child = right.get_id()}};
                        update_data(std::move(keys), std::move(pointers));
                        return seastar::when_all_succeed(cache(shared_from_this()), cache(left), cache(right)).discard_result();
                    });
                });
    }
    auto separator = _keys[midpoint].clone();
    update_data(std::move(left_keys), std::move(left_pointers));
    return create_node(std::move(right_keys), std::move(right_pointers)).then([this, separator{std::move(separator)}](auto sibling) {
        sibling.set_high_key(std::move(_high_key));
        _high_key = separator.clone();
        return link_siblings(shared_from_this(), sibling).then([this, sibling] {
            return get_parent().then([this, sibling](auto parent) {
                SPIDERDB_LOGGER_DEBUG("Node {:0>12} - Split to {}", _page.get_id(), sibling.get_id());
                sibling.set_parent_node(parent.get_pointer());
                return parent.promote(_high_key.clone(), _page.get_id(), sibling.get_id()).then([this, sibling, parent] {
                    return seastar::when_all_succeed(cache(parent), cache(shared_from_this()), cache(sibling)).discard_result();
                });
            });
        });
    });
}

bool node_impl::need_split() noexcept {
    if (_keys.size() < _btree->get_config().min_keys_on_each_node) {
        return false;
    }
    if (_keys.size() > _btree->get_config().max_keys_on_each_node) {
        return true;
    }
    auto work_size = _btree->get_file().get_config().page_size - _btree->get_file().get_config().page_header_size;
    if (calculate_data_length() > work_size) {
        if (calculate_data_length(true) > work_size) {
            return true;
        }
    }
    return false;
}

seastar::future<> node_impl::promote(string&& promoted_key, node_id left_child, node_id right_child) {
    auto it = std::find_if(_pointers.begin(), _pointers.end(), [left_child](const auto& pointer) {
        return pointer.child == left_child;
    });
    if (it == _pointers.end()) {
        return seastar::make_exception_future<>(std::runtime_error("Child not existed"));
    }
    auto id = it - _pointers.begin();
    _keys.insert(_keys.begin() + id, promoted_key);
    _pointers.insert(_pointers.begin() + id + 1, pointer{.child = right_child});
    update_metadata();
    _data_len += promoted_key.length() + sizeof(uint32_t) + sizeof(pointer);
    SPIDERDB_LOGGER_DEBUG("Node {:0>12} - Promoted key {}", _page.get_id(), promoted_key);
    return seastar::futurize<void>::invoke([this] {
        if (!need_split()) {
            return seastar::now();
        }
        return split();
    }).then([this] {
        return cache(shared_from_this());
    });
}

seastar::future<> node_impl::merge() {
    if (!_btree) {
        return seastar::make_exception_future<>(std::runtime_error("Closed error"));
    }
    if (_keys.empty() && _pointers.empty()) {
        return invalidate();
    }
    auto left_ptr = seastar::make_lw_shared<node>();
    auto right_ptr = seastar::make_lw_shared<node>();
    return seastar::futurize<void>::invoke([this, left_ptr, right_ptr] {
        if (_prev == null_node) {
            return seastar::now();
        }
        return _btree->get_node(_prev).then([this, left_ptr, right_ptr](auto prev) {
            if (prev.get_parent_node() == _header._parent && prev.need_merge()) {
                *left_ptr = prev;
                *right_ptr = shared_from_this();
            }
            return seastar::now();
        });
    }).then([this, left_ptr, right_ptr] {
        if (_next == null_node) {
            return seastar::now();
        }
        if (*left_ptr && *right_ptr) {
            return seastar::now();
        }
        return _btree->get_node(_next).then([this, left_ptr, right_ptr](auto next) {
            if (next.get_parent_node() == _header._parent && next.need_merge()) {
                *left_ptr = shared_from_this();
                *right_ptr = next;
            }
            return seastar::now();
        });
    }).then([this, left_ptr, right_ptr] {
        if (!*left_ptr || !*right_ptr) {
            return seastar::now();
        }
        return get_parent().then([this, left_ptr, right_ptr](auto parent) {
            return parent.demote(left_ptr->get_id(), right_ptr->get_id()).then([this, parent, left_ptr, right_ptr](auto&& demoted_key) {
                // Prepare data
                std::vector<string> keys;
                std::vector<pointer> pointers;
                const auto& left_keys = left_ptr->get_key_list();
                const auto& right_keys = right_ptr->get_key_list();
                const auto& left_pointers = left_ptr->get_pointer_list();
                const auto& right_pointers = right_ptr->get_pointer_list();
                const auto& left_high_key = left_ptr->get_high_key();
                const auto& right_high_key = right_ptr->get_high_key();
                switch (_page.get_type()) {
                    case node_type::internal: {
                        keys.reserve(left_keys.size() + right_keys.size() + 1);
                        keys.insert(keys.end(), left_keys.begin(), left_keys.end());
                        keys.insert(keys.end(), right_keys.begin(), right_keys.end());
                        break;
                    }
                    case node_type::leaf: {
                        keys.reserve(left_keys.size() + right_keys.size());
                        keys.insert(keys.end(), left_keys.begin(), left_keys.end());
                        keys.push_back(demoted_key);
                        keys.insert(keys.end(), right_keys.begin(), right_keys.end());
                        break;
                    }
                    default: {
                        return seastar::make_exception_future<>(std::runtime_error("Wrong page"));
                    }
                }
                pointers.reserve(left_pointers.size() + right_pointers.size());
                pointers.insert(pointers.end(), left_pointers.begin(), left_pointers.end());
                pointers.insert(pointers.end(), right_pointers.begin(), right_pointers.end());
                // Merge
                left_ptr->update_data(std::move(keys), std::move(pointers));
                left_ptr->set_high_key(right_high_key.clone());
                return left_ptr->become_parent().then([this, parent, left_ptr, right_ptr] {
                    SPIDERDB_LOGGER_DEBUG("Node {:0>12} - Merged {} to {}", _page.get_id(), left_ptr->get_id(), right_ptr->get_id());
                    if (right_ptr->get_next_node() == null_node) {
                        left_ptr->set_next_node(null_node);
                        return seastar::now();
                    }
                    return _btree->get_node(right_ptr->get_next_node()).then([this, parent, left_ptr](auto new_right) {
                        return link_siblings(*left_ptr, new_right).then([this, parent, left_ptr, new_right] {
                            return seastar::when_all_succeed(cache(parent), cache(*left_ptr), cache(new_right)).discard_result();
                        });
                    }).then([right_ptr] {
                        return right_ptr->clean().finally([right_ptr] {});
                    });
                });
            });
        });
    });
}

bool node_impl::need_merge() noexcept {
    if (_keys.size() < _btree->get_config().min_keys_on_each_node / 2) {
        return true;
    }
    auto work_size = _btree->get_file().get_config().page_size - _btree->get_file().get_config().page_header_size;
    if (calculate_data_length() < work_size / 2) {
        if (calculate_data_length(true) < work_size / 2) {
            return true;
        }
    }
    return false;
}

seastar::future<string> node_impl::demote(node_id left_child, node_id right_child) {
    auto it = std::find_if(_pointers.begin(), _pointers.end(), [left_child](const auto& pointer) {
        return pointer.child == left_child;
    });
    if (it == _pointers.end()) {
        return seastar::make_exception_future<string>(std::runtime_error("Child not existed"));
    }
    auto id = it - _pointers.begin();
    string demoted_key;
    if (left_child == right_child) {
        if (id == 0 && !_keys.empty()) {
            demoted_key = std::move(_keys[0]);
            _keys.erase(_keys.begin());
        } else if (id - 1 >= 0 && id - 1 < _keys.size()) {
            demoted_key = std::move(_keys[id - 1]);
            _keys.erase(_keys.begin() + id - 1);
        }
        _pointers.erase(_pointers.begin() + id);
    } else if (std::next(it) != _pointers.end() && std::next(it)->child == right_child) {
        demoted_key = std::move(_keys[id]);
        _keys.erase(_keys.begin() + id);
        _pointers.erase(_pointers.begin() + id + 1);
    } else {
        return seastar::make_exception_future<string>(std::runtime_error("Child not existed"));
    }
    update_metadata();
    _data_len -= demoted_key.length() + sizeof(uint32_t) + sizeof(pointer);
    SPIDERDB_LOGGER_DEBUG("Node {:0>12} - Demoted key {}", _page.get_id(), demoted_key);
    return seastar::futurize<void>::invoke([this] {
        if (!need_merge()) {
            return seastar::now();
        }
        return merge();
    }).then([this] {
        return cache(shared_from_this());
    }).then([demoted_key{std::move(demoted_key)}] {
        return seastar::make_ready_future<string>(demoted_key);
    });
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

seastar::future<node> node_impl::create_node(std::vector<string>&& keys, std::vector<pointer>&& pointers) {
    if (!_btree) {
        return seastar::make_exception_future<node>(std::runtime_error("Closed error"));
    }
    return _btree->create_node(_page.get_type()).then([keys{std::move(keys)}, pointers{std::move(pointers)}](auto child) mutable {
        child.update_data(std::move(keys), std::move(pointers));
        return child.become_parent().then([child] {
            return seastar::make_ready_future<node>(child);
        });
    });
}

seastar::future<> node_impl::link_siblings(node left, node right) {
    if (!_btree) {
        return seastar::make_exception_future<>(std::runtime_error("Closed error"));
    }
    return seastar::futurize<void>::invoke([this, left, right] {
        if (left.get_next_node() != null_node) {
            right.set_next_node(left.get_next_node());
            return _btree->get_node(left.get_next_node()).then([this, right](auto old_right) {
                old_right.set_prev_node(right.get_id());
                return cache(old_right);
            });
        }
        return seastar::now();
    }).then([left, right] {
        left.set_next_node(right.get_id());
        right.set_prev_node(left.get_id());
    });
}

seastar::future<> node_impl::cache(node node) {
    if (!_btree) {
        return seastar::make_exception_future<>(std::runtime_error("Closed error"));
    }
    return _btree->cache_node(node);
}

seastar::future<> node_impl::become_parent() {
    if (!_btree) {
        return seastar::make_exception_future<>(std::runtime_error("Closed error"));
    }
    if (_page.get_type() != node_type::internal) {
        return seastar::now();
    }
    return seastar::parallel_for_each(_pointers, [this](auto pointer) {
        return _btree->get_node(pointer.child, weak_from_this()).then([this](auto child) {
             child.set_parent_node(weak_from_this());
             return cache(child);
        });
    });
}

void node_impl::update_data(std::vector<string>&& keys, std::vector<pointer>&& pointers) noexcept {
    _keys = std::move(keys);
    _pointers = std::move(pointers);
    update_metadata();
    calculate_data_length(true);
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

seastar::future<> node_impl::invalidate() {
    if (!_btree) {
        return seastar::make_exception_future<>(std::runtime_error("Closed error"));
    }
    if (_page.get_id() == _btree->get_root().get_id()) {
        return seastar::now();
    }
    if (!_keys.empty() || !_pointers.empty()) {
        return seastar::now();
    }
    return get_parent().then([this](auto parent) {
        return parent.demote(_page.get_id(), _page.get_id()).then([this, parent](auto) {
            return cache(parent);
        });
    }).then([this] {
        if (_prev == null_node) {
            return seastar::now();
        }
        return _btree->get_node(_prev).then([this](auto prev) {
            prev.set_next_node(_next);
            return cache(prev);
        });
    }).then([this] {
        if (_next == null_node) {
            return seastar::now();
        }
        return _btree->get_node(_next).then([this](auto next) {
            next.set_prev_node(_prev);
            return cache(next);
        });
    }).then([this] {
        return clean();
    });
}

seastar::future<> node_impl::clean() {
    update_data({}, {});
    _parent = nullptr;
    _next = null_node;
    _prev = null_node;
    _prefix = string{};
    _high_key = string{};
    _data_len = 0;
    _header._parent = null_node;
    _header._key_count = 0;
    _header._prefix_len = 0;
    SPIDERDB_LOGGER_DEBUG("Node {:0>12} - Cleaned", _page.get_id());
    return _btree->get_file().unlink_pages_from(_page);
}

}