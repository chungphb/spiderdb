//
// Created by chungphb on 15/5/21.
//

#include <spiderdb/core/storage.h>
#include <spiderdb/util/log.h>

namespace spiderdb {

available_page_list::available_page_list(size_t capacity) : _capacity{capacity} {}

void available_page_list::add(page_id id, uint32_t available_space) {
    _available_pages.insert_or_assign(id, available_space);
}

void available_page_list::remove(page_id id) {
    _available_pages.erase(id);
}

page_id available_page_list::find(uint32_t required_space) {
    auto it = std::find_if(_available_pages.begin(), _available_pages.end(), [required_space](const auto& page) {
        return page.second >= required_space;
    });
    if (it == _available_pages.end()) {
        return null_page;
    }
    return it->first;
}

seastar::future<> available_page_list::write(seastar::temporary_buffer<char> buffer) {
    uint64_t size = std::min(_available_pages.size(), _capacity);
    memcpy(buffer.get_write(), &size, sizeof(size));
    buffer.trim_front(sizeof(size));
    auto it = _available_pages.begin();
    for (size_t i = 0; i < size; ++i) {
        memcpy(buffer.get_write(), &it->first, sizeof(it->first));
        buffer.trim_front(sizeof(it->first));
        memcpy(buffer.get_write(), &it->second, sizeof(it->second));
        buffer.trim_front(sizeof(it->second));
        it = std::next(it);
    }
    return seastar::now();
}

seastar::future<> available_page_list::read(seastar::temporary_buffer<char> buffer) {
    uint64_t size;
    memcpy(&size, buffer.begin(), sizeof(size));
    buffer.trim_front(sizeof(size));
    size = std::min(size, _capacity);
    for (size_t i = 0; i < size; ++i) {
        page_id page;
        memcpy(&page, buffer.begin(), sizeof(page));
        buffer.trim_front(sizeof(page));
        uint32_t available_space;
        memcpy(&available_space, buffer.begin(), sizeof(available_space));
        buffer.trim_front(sizeof(available_space));
        add(page, available_space);
    }
    return seastar::now();
}

size_t available_page_list::size() const noexcept {
    uint64_t size = std::min(_available_pages.size(), _capacity);
    return sizeof(size) + size * (sizeof(page_id) + sizeof(uint32_t));
}

seastar::future<> storage_header::write(seastar::temporary_buffer<char> buffer) {
    return btree_header::write(buffer.share()).then([this, buffer{buffer.share()}]() mutable {
        if (!_available_page_list) {
            return seastar::now();
        }
        buffer.trim_front(btree_header::size());
        return _available_page_list->write(buffer.share());
    });
}

seastar::future<> storage_header::read(seastar::temporary_buffer<char> buffer) {
    return btree_header::read(buffer.share()).then([this, buffer{buffer.share()}]() mutable {
        if (!_available_page_list) {
            return seastar::now();
        }
        buffer.trim_front(btree_header::size());
        return _available_page_list->read(buffer.share());
    });
}

storage_impl::storage_impl(std::string name, spiderdb_config config) : btree_impl{std::move(name), config} {
    _config = static_cast<storage_config&>(config);
}

seastar::future<> storage_impl::open() {
    if (is_open()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::file_already_opened});
    }
    return btree_impl::open().then([this] {
        _storage_header = seastar::dynamic_pointer_cast<storage_header>(_btree_header);
        _storage_header->_available_page_list = std::make_unique<available_page_list>(_config.max_available_pages);
        auto evictor = [](const std::pair<page_id, data_page>& evicted_item) -> seastar::future<> {
            auto evicted_data_page = evicted_item.second;
            return evicted_data_page.flush().finally([evicted_data_page] {});
        };
        _cache = std::make_unique<cache<page_id, data_page>>(_config.n_cached_data_pages, std::move(evictor));
        SPIDERDB_LOGGER_INFO("Created storage");
    });
}

seastar::future<> storage_impl::flush() {
    return seastar::parallel_for_each(_cache->get_all_items(), [](auto item) {
        auto data_page = item.second;
        return data_page.flush().finally([data_page] {});
    }).then([this] {
        return _cache->clear();
    }).then([this] {
        return btree_impl::flush();
    });
}

seastar::future<> storage_impl::close() {
    if (!is_open()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::file_already_closed});
    }
    return btree_impl::close().then([] {
        SPIDERDB_LOGGER_INFO("Closed storage");
    });
}

seastar::future<> storage_impl::insert(string&& key, string&& value) {
    return add_value(std::move(value)).then([this, key{std::move(key)}](auto ptr) mutable {
        return add(std::move(key), ptr).handle_exception([this, ptr](auto ex) {
            return remove_value(ptr).then([ex] {
                return seastar::make_exception_future<>(ex);
            });
        });
    });
}

seastar::future<> storage_impl::update(string&& key, string&& value) {
    return find(key.clone()).then([this, value{value.clone()}](auto ptr) mutable {
        return update_value(ptr, std::move(value));
    });
}

seastar::future<> storage_impl::erase(string&& key) {
    return remove(std::move(key)).then([this](auto ptr) {
        return remove_value(ptr);
    });
}

seastar::future<string> storage_impl::select(string&& key) {
    return find(key.clone()).then([this, key{std::move(key)}](auto ptr) mutable {
        return find_value(ptr);
    });
}

void storage_impl::log() const noexcept {
    btree_impl::log();
}

bool storage_impl::is_open() const noexcept {
    return btree_impl::is_open();
}

seastar::shared_ptr<file_header> storage_impl::get_new_file_header() {
    return seastar::make_shared<storage_header>();
}

seastar::shared_ptr<page_header> storage_impl::get_new_page_header() {
    return seastar::make_shared<data_page_header>();
}

seastar::weak_ptr<storage_impl> storage_impl::get_pointer() noexcept {
    return seastar::weakly_referencable<storage_impl>::weak_from_this();
}

seastar::future<data_page> storage_impl::create_data_page() {
    return get_free_page().then([this](auto page) mutable {
        data_page new_data_page{page, get_pointer()};
        new_data_page.get_page().set_type(page_type::data);
        _data_pages.emplace(new_data_page.get_id(), new_data_page.get_pointer());
        return cache_data_page(new_data_page).then([this, new_data_page] {
            SPIDERDB_LOGGER_DEBUG("Data page {:0>12} - Created", new_data_page.get_id());
            return seastar::make_ready_future<data_page>(new_data_page);
        });
    });
}

seastar::future<data_page> storage_impl::get_data_page(page_id id) {
    return seastar::futurize_invoke([this, id] {
        // If data page is still on data page cache
        return _cache->get(id).then([](auto cached_data_page) {
            return seastar::make_ready_future<data_page>(cached_data_page);
        }).handle_exception([this, id](auto ex) {
            return seastar::with_semaphore(_get_data_page_lock, 1, [this, id] {
                // If data page has not been flushed
                auto data_page_it = _data_pages.find(id);
                if (data_page_it != _data_pages.end()) {
                    if (data_page_it->second) {
                        return seastar::make_ready_future<data_page>(data_page_it->second->shared_from_this());
                    }
                    _data_pages.erase(data_page_it);
                }
                // Otherwise
                return get_or_create_page(id).then([this](auto page) {
                    auto loading_data_page = data_page{page, get_pointer()};
                    return loading_data_page.load().then([this, loading_data_page] {
                        _data_pages.emplace(loading_data_page.get_id(), loading_data_page.get_pointer());
                        return seastar::make_ready_future<data_page>(loading_data_page);
                    });
                });
            });
        });
    }).then([this](auto loaded_data_page) mutable {
        return cache_data_page(loaded_data_page).then([loaded_data_page] {
            return seastar::make_ready_future<data_page>(loaded_data_page);
        });
    });
}

seastar::future<> storage_impl::cache_data_page(data_page data_page) {
    return _cache->put(data_page.get_id(), data_page);
}

seastar::future<data_pointer> storage_impl::add_value(string&& value) {
    auto required_space = sizeof(uint32_t) + value.length();
    auto available_page = _storage_header->_available_page_list->find(required_space);
    return seastar::futurize_invoke([this, available_page] {
        if (available_page != null_page) {
            return get_data_page(available_page);
        } else {
            return create_data_page();
        }
    }).then([this, value{std::move(value)}](auto data_page) mutable {
        return data_page.add(std::move(value)).then([this, data_page](auto vid) {
            // Update available page list
            uint32_t available_space = 0;
            if (data_page.get_data_length() < data_page.get_page().get_work_size()) {
                available_space = static_cast<uint32_t>(data_page.get_page().get_work_size() - data_page.get_data_length());
            }
            if (available_space >= _config.min_available_space) {
                _storage_header->_available_page_list->add(data_page.get_id(), available_space);
            }
            // Generate data pointer
            return seastar::make_ready_future<data_pointer>(generate_data_pointer(data_page.get_id(), vid));
        });
    });
}

seastar::future<> storage_impl::update_value(data_pointer ptr, string&& value) {
    return get_data_page(get_page_id(ptr)).then([this, ptr, value{std::move(value)}](auto data_page) mutable {
        return data_page.update(get_value_id(ptr), std::move(value)).finally([data_page] {});
    });
}

seastar::future<> storage_impl::remove_value(data_pointer ptr) {
    return get_data_page(get_page_id(ptr)).then([this, ptr](auto data_page) mutable {
        return data_page.remove(get_value_id(ptr)).finally([data_page] {});
    });
}

seastar::future<string> storage_impl::find_value(data_pointer ptr) {
    return get_data_page(get_page_id(ptr)).then([this, ptr](auto data_page) mutable {
        return data_page.find(get_value_id(ptr)).finally([data_page] {});
    });
}

data_pointer storage_impl::generate_data_pointer(page_id pid, value_id vid) {
    assert(pid >= 0 && pid < 0x7fffffffffff);
    assert(vid >= 0 && vid < 0x7fffffffffff);
    return ((pid & 0xffffffffffff) << 16) | vid;
}

page_id storage_impl::get_page_id(data_pointer ptr) {
    return ptr >> 16;
}

value_id storage_impl::get_value_id(data_pointer ptr) {
    return static_cast<value_id>(ptr & 0xffff);
}

storage::storage(std::string name, spiderdb_config config) {
    _impl = seastar::make_lw_shared<storage_impl>(std::move(name), config);
}

storage::storage(const storage& other_storage) {
    _impl = other_storage._impl;
}

storage::storage(storage&& other_storage) noexcept {
    _impl = std::move(other_storage._impl);
}

storage& storage::operator=(const storage& other_storage) {
    _impl = other_storage._impl;
    return *this;
}

storage& storage::operator=(storage&& other_storage) noexcept {
    _impl = std::move(other_storage._impl);
    return *this;
}

const storage_config& storage::get_config() const {
    if (!_impl) {
        throw spiderdb_error{error_code::invalid_storage};
    }
    return _impl->_config;
}

seastar::future<> storage::open() const {
    if (!_impl) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::invalid_storage});
    }
    return _impl->open();
}

seastar::future<> storage::close() const {
    if (!_impl) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::invalid_storage});
    }
    if (!_impl->is_open()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::file_already_closed});
    }
    return _impl->close();
}

seastar::future<> storage::insert(string&& key, string&& value) {
    if (!_impl) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::invalid_storage});
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
    if (value.empty()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::empty_value});
    }
    return _impl->insert(std::move(key), std::move(value));
}

seastar::future<> storage::update(string&& key, string&& value) {
    if (!_impl) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::invalid_btree});
    }
    if (!_impl->is_open()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::file_already_closed});
    }
    if (key.empty()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::empty_key});
    }
    if (value.empty()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::empty_value});
    }
    return _impl->update(std::move(key), std::move(value));
}

seastar::future<> storage::erase(string&& key) {
    if (!_impl) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::invalid_btree});
    }
    if (!_impl->is_open()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::file_already_closed});
    }
    if (key.empty()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::empty_key});
    }
    return _impl->erase(std::move(key));
}

seastar::future<string> storage::select(string&& key) {
    if (!_impl) {
        return seastar::make_exception_future<string>(spiderdb_error{error_code::invalid_btree});
    }
    if (!_impl->is_open()) {
        return seastar::make_exception_future<string>(spiderdb_error{error_code::file_already_closed});
    }
    if (key.empty()) {
        return seastar::make_exception_future<string>(spiderdb_error{error_code::empty_key});
    }
    return _impl->select(std::move(key));
}

void storage::log() const {
    if (!_impl) {
        throw spiderdb_error{error_code::invalid_btree};
    }
    if (!_impl->is_open()) {
        throw spiderdb_error{error_code::file_already_closed};
    }
    _impl->log();
}

}