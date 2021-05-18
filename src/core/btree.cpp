//
// Created by chungphb on 4/5/21.
//

#include <spiderdb/core/btree.h>
#include <spiderdb/util/log.h>

namespace spiderdb {

seastar::future<> btree_header::write(seastar::temporary_buffer<char> buffer) {
    return file_header::write(buffer.share()).then([this, buffer{buffer.share()}]() mutable {
        buffer.trim_front(file_header::size());
        memcpy(&_root, buffer.begin(), sizeof(_root));
        buffer.trim_front(sizeof(_root));
        return seastar::now();
    });
}

seastar::future<> btree_header::read(seastar::temporary_buffer<char> buffer) {
    return file_header::read(buffer.share()).then([this, buffer{buffer.share()}]() mutable {
        buffer.trim_front(file_header::size());
        memcpy(buffer.get_write(), &_root, sizeof(_root));
        buffer.trim_front(sizeof(_root));
        return seastar::now();
    });
}

btree_impl::btree_impl(std::string name, spiderdb_config config) : file_impl{std::move(name), static_cast<file_config&>(config)} {
    _config = static_cast<btree_config&>(config);
}

node btree_impl::get_root() const noexcept {
    return _root;
}

const btree_config& btree_impl::get_config() const noexcept {
    return _config;
}

seastar::future<> btree_impl::open() {
    if (is_open()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::file_already_opened});
    }
    return file_impl::open().then([this] {
        _btree_header = seastar::dynamic_pointer_cast<btree_header>(_file_header);
        auto evictor = [](const std::pair<node_id, node>& evicted_item) -> seastar::future<> {
            auto evicted_node = evicted_item.second;
            return evicted_node.flush().finally([evicted_node] {});
        };
        _cache = std::make_unique<cache<node_id, node>>(_config.n_cached_nodes, std::move(evictor));
        return get_or_create_page(_btree_header->_root).then([this](auto root) {
            _btree_header->_page_count++;
            _btree_header->_dirty = true;
            _root = node{root, get_pointer()};
            SPIDERDB_LOGGER_DEBUG("Root {:0>12} - Created", _root.get_id());
            return seastar::futurize_invoke([this, root] {
                switch (root.get_type()) {
                    case page_type::unused: {
                        _root.get_page().set_type(page_type::leaf);
                        _root.mark_dirty();
                        return seastar::now();
                    }
                    case page_type::leaf:
                    case page_type::internal: {
                        return _root.load();
                    }
                    default: {
                        return seastar::make_exception_future<>(spiderdb_error{error_code::invalid_page_type});
                    }
                }
            }).then([this] {
                SPIDERDB_LOGGER_INFO("Created B-Tree");
                return cache_node(_root);
            });
        });
    });
}

seastar::future<> btree_impl::flush() {
    return seastar::parallel_for_each(_cache->get_all_items(), [](auto item) {
        auto node = item.second;
        return node.flush().finally([node] {});
    }).then([this] {
        return _cache->clear();
    }).then([this] {
        return file_impl::flush();
    });
}

seastar::future<> btree_impl::close() {
    if (!btree_impl::is_open()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::file_already_closed});
    }
    auto root = std::move(_root);
    return root.flush().finally([this, root] {
        return file_impl::close().then([] {
            SPIDERDB_LOGGER_INFO("Closed B-Tree");
        });
    });
}

seastar::future<> btree_impl::add(string&& key, data_pointer ptr) {
    return _root.add(std::move(key), ptr);
}

seastar::future<data_pointer> btree_impl::remove(string&& key) {
    return _root.remove(std::move(key));
}

seastar::future<data_pointer> btree_impl::find(string&& key) {
    return _root.find(std::move(key));
}

seastar::future<node> btree_impl::create_node(node_type type, seastar::weak_ptr<node_impl>&& parent) {
    return get_free_page().then([this, type, parent{std::move(parent)}](auto page) mutable {
        node new_node{page, get_pointer(), std::move(parent)};
        new_node.get_page().set_type(type);
        _nodes.emplace(new_node.get_id(), new_node.get_pointer());
        return cache_node(new_node).then([this, new_node] {
            SPIDERDB_LOGGER_DEBUG("Node {:0>12} - Created", new_node.get_id());
            return seastar::make_ready_future<node>(new_node);
        });
    });
}

seastar::future<node> btree_impl::get_node(node_id id, seastar::weak_ptr<node_impl>&& parent) {
    return seastar::futurize_invoke([this, id] {
        // If node is still on node cache
        return _cache->get(id).then([](auto cached_node) {
            return seastar::make_ready_future<node>(cached_node);
        }).handle_exception([this, id](auto ex) {
            return seastar::with_semaphore(_get_node_lock, 1, [this, id] {
                // If node has not been flushed
                auto node_it = _nodes.find(id);
                if (node_it != _nodes.end()) {
                    if (node_it->second) {
                        return seastar::make_ready_future<node>(node_it->second->shared_from_this());
                    }
                    _nodes.erase(node_it);
                }
                // Otherwise
                return get_or_create_page(id).then([this](auto page) {
                    auto loading_node = node{page, get_pointer()};
                    return loading_node.load().then([this, loading_node] {
                        _nodes.emplace(loading_node.get_id(), loading_node.get_pointer());
                        return seastar::make_ready_future<node>(loading_node);
                    });
                });
            });
        });
    }).then([this, parent{std::move(parent)}](auto loaded_node) mutable {
        loaded_node.update_parent(std::move(parent));
        return cache_node(loaded_node).then([loaded_node] {
            return seastar::make_ready_future<node>(loaded_node);
        });
    });
}

seastar::future<> btree_impl::cache_node(node node) {
    return _cache->put(node.get_id(), node);
}

void btree_impl::log() const noexcept {
    file_impl::log();
    SPIDERDB_LOGGER_TRACE("\t{:<18}{:>20}", "Root: ", _btree_header->_root);
}

bool btree_impl::is_open() const noexcept {
    return (bool)_root && file_impl::is_open();
}

seastar::shared_ptr<file_header> btree_impl::get_new_file_header() {
    return seastar::make_shared<btree_header>();
}

seastar::shared_ptr<page_header> btree_impl::get_new_page_header() {
    return seastar::make_shared<node_header>();
}

seastar::weak_ptr<btree_impl> btree_impl::get_pointer() noexcept {
    return seastar::weakly_referencable<btree_impl>::weak_from_this();
}

btree::btree(std::string name, spiderdb_config config) {
    _impl = seastar::make_lw_shared<btree_impl>(std::move(name), config);
}

btree::btree(const btree& other_btree) {
    _impl = other_btree._impl;
}

btree::btree(btree&& other_btree) noexcept {
    _impl = std::move(other_btree._impl);
}

btree& btree::operator=(const btree& other_btree) {
    _impl = other_btree._impl;
    return *this;
}

btree& btree::operator=(btree&& other_btree) noexcept {
    _impl = std::move(other_btree._impl);
    return *this;
}

const btree_config& btree::get_config() const {
    if (!_impl) {
        throw spiderdb_error{error_code::invalid_btree};
    }
    return _impl->_config;
}

seastar::future<> btree::open() const {
    if (!_impl) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::invalid_file});
    }
    return _impl->open();
}

seastar::future<> btree::close() const {
    if (!_impl) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::invalid_btree});
    }
    if (!_impl->is_open()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::file_already_closed});
    }
    return _impl->close();
}

seastar::future<> btree::add(string&& key, data_pointer ptr) const {
    if (!_impl) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::invalid_btree});
    }
    if (!_impl->is_open()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::file_already_closed});
    }
    if (key.empty()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::empty_key});
    }
    if (key.length() > _impl->get_root().get_page().get_work_size() / _impl->get_config().min_keys_on_each_node) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::key_too_long});
    }
    return _impl->add(std::move(key), ptr);
}

seastar::future<data_pointer> btree::remove(string&& key) const {
    if (!_impl) {
        return seastar::make_exception_future<data_pointer>(spiderdb_error{error_code::invalid_btree});
    }
    if (!_impl->is_open()) {
        return seastar::make_exception_future<data_pointer>(spiderdb_error{error_code::file_already_closed});
    }
    if (key.empty()) {
        return seastar::make_exception_future<data_pointer>(spiderdb_error{error_code::empty_key});
    }
    return _impl->remove(std::move(key));
}

seastar::future<data_pointer> btree::find(string&& key) const {
    if (!_impl) {
        return seastar::make_exception_future<data_pointer>(spiderdb_error{error_code::invalid_btree});
    }
    if (!_impl->is_open()) {
        return seastar::make_exception_future<data_pointer>(spiderdb_error{error_code::file_already_closed});
    }
    if (key.empty()) {
        return seastar::make_exception_future<data_pointer>(spiderdb_error{error_code::empty_key});
    }
    return _impl->find(std::move(key));
}

void btree::log() const {
    if (!_impl) {
        throw spiderdb_error{error_code::invalid_btree};
    }
    if (!_impl->is_open()) {
        throw spiderdb_error{error_code::file_already_closed};
    }
    _impl->log();
}

}