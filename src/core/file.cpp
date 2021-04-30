//
// Created by chungphb on 27/4/21.
//

#include <spiderdb/core/file.h>
#include <spiderdb/util/log.h>
#include <seastar/core/seastar.hh>

namespace spiderdb {

seastar::future<> file_header::flush(seastar::file file) {
    if (!file || !_dirty) {
        return seastar::now();
    }
    seastar::temporary_buffer<char> buffer{_size};
    return seastar::do_with(std::move(buffer), [this, file](auto& buffer) mutable {
        return read(buffer.share()).then([this, file, buffer{buffer.share()}]() mutable {
            return file.dma_write(0, buffer.begin(), buffer.size()).then([this](auto) {
                _dirty = false;
            });
        });
    });
}

seastar::future<> file_header::load(seastar::file file) {
    if (!file) {
        return seastar::now();
    }
    return file.dma_read_exactly<char>(0, _size).then([this](auto buffer) {
        return write(buffer.share());
    });
}

seastar::future<> file_header::write(seastar::temporary_buffer<char> buffer) {
    memcpy(&_page_size, buffer.begin(), sizeof(_page_size));
    buffer.trim_front(sizeof(_page_size));
    memcpy(&_page_count, buffer.begin(), sizeof(_page_count));
    buffer.trim_front(sizeof(_page_count));
    memcpy(&_first_free_page, buffer.begin(), sizeof(_first_free_page));
    buffer.trim_front(sizeof(_first_free_page));
    memcpy(&_last_free_page, buffer.begin(), sizeof(_last_free_page));
    buffer.trim_front(sizeof(_last_free_page));
    return seastar::now();
}

seastar::future<> file_header::read(seastar::temporary_buffer<char> buffer) {
    memset(buffer.get_write(), 0, _size);
    memcpy(buffer.get_write(), &_page_size, sizeof(_page_size));
    buffer.trim_front(sizeof(_page_size));
    memcpy(buffer.get_write(), &_page_count, sizeof(_page_count));
    buffer.trim_front(sizeof(_page_count));
    memcpy(buffer.get_write(), &_first_free_page, sizeof(_first_free_page));
    buffer.trim_front(sizeof(_first_free_page));
    memcpy(buffer.get_write(), &_last_free_page, sizeof(_last_free_page));
    buffer.trim_front(sizeof(_last_free_page));
    return seastar::now();
}

file_impl::file_impl(std::string name, file_config config) : _name{std::move(name)}, _config{config} {
    _header._size = _config.file_header_size;
    _header._page_size = _config.page_size;
    spiderdb_logger.set_level(_config.log_level);
}

file_impl::~file_impl() {
    if (_lock.available_units() == 0) {
        SPIDERDB_LOGGER_ERROR("File not closed");
        _lock.signal();
    }
}

seastar::future<> file_impl::open() {
    if (!_lock.try_wait() || is_open()) {
        SPIDERDB_LOGGER_WARN("File already opened");
        return seastar::now();
    }
    return seastar::file_exists(_name).then([this](auto exists) {
        return seastar::open_file_dma(_name, seastar::open_flags::create | seastar::open_flags::rw).then([this, exists](auto file) {
            _file = file;
            if (exists) {
                SPIDERDB_LOGGER_INFO("Opened file: {}", _name);
                return _header.load(file);
            } else {
                SPIDERDB_LOGGER_INFO("Created file: {}", _name);
                return _header.flush(file);
            }
        });
    });
}

seastar::future<> file_impl::flush() {
    return _header.flush(_file);
}

seastar::future<> file_impl::close() {
    return flush().then([this] {
        if (!is_open()) {
            SPIDERDB_LOGGER_WARN("File already closed");
            return seastar::now();
        }
        auto file = std::move(_file);
        return file.close().finally([this] {
            SPIDERDB_LOGGER_INFO("Closed file: {}", _name);
            _lock.signal();
        });
    });
}

seastar::future<> file_impl::write(string data) {
    return get_free_page().then([this, data{std::move(data)}](auto free_page) mutable {
        return write(free_page, std::move(data));
    });
}

seastar::future<string> file_impl::read() {
    return read(0);
}

void file_impl::log() const noexcept {
    SPIDERDB_LOGGER_DEBUG("\t{:<20}|{:>20}", "FILE", _name);
    SPIDERDB_LOGGER_DEBUG("\t{:<20}|{:>20}", "Page size", _header._page_size);
    SPIDERDB_LOGGER_DEBUG("\t{:<20}|{:>20}", "Page count", _header._page_count);
    SPIDERDB_LOGGER_DEBUG("\t{:<20}|{:>20}", "First free page", _header._first_free_page);
    SPIDERDB_LOGGER_DEBUG("\t{:<20}|{:>20}", "Last free page", _header._last_free_page);
}

bool file_impl::is_open() const noexcept {
    return (bool)_file;
}

seastar::future<> file_impl::write(page_id id, string data) {
    return get_or_create_page(id).then([this, data{std::move(data)}](auto page) mutable {
        return write(page, std::move(data));
    });
}

seastar::future<string> file_impl::read(page_id id) {
    return get_or_create_page(id).then([this](auto page) mutable {
        return read(page);
    });
}

seastar::future<page> file_impl::get_free_page() {
    return seastar::with_semaphore(_get_free_page_lock, 1, [this] {
        if (_header._first_free_page != null_page) {
            return get_or_create_page(_header._first_free_page);
        } else {
            return get_or_create_page(_header._page_count++);
        }
    }).then([this](auto free_page) {
        _header._first_free_page = free_page.get_next_page();
        if (_header._first_free_page == null_page) {
            _header._last_free_page = null_page;
        }
        _header._dirty = true;
        free_page.set_next_page(null_page);
        free_page.set_type(page_type::unused);
        return seastar::make_ready_future<page>(free_page);
    });
}

seastar::future<page> file_impl::get_or_create_page(page_id id) {
    if (id < 0 || id > _header._page_count) {
        return seastar::make_exception_future<page>(std::runtime_error("Invalid access"));
    }
    auto page_it = _pages.find(id);
    if (page_it != _pages.end() && page_it->second) {
        return seastar::make_ready_future<page>(page_it->second->shared_from_this());
    }
    page new_page{id, _config};
    _pages.emplace(id, new_page.get_pointer());
    return new_page.load(_file).then([this, new_page] {
        return seastar::make_ready_future<page>(new_page);
    });

}

seastar::future<> file_impl::unlink_pages_from(page_id id) {
    return get_or_create_page(id).then([this](auto page) {
        return unlink_pages_from(page);
    });
}

seastar::future<> file_impl::write(page first, string data) {
    return seastar::do_with(data.get_input_stream(), [this, first](auto& is) mutable {
        first.set_record_length(is.size());
        auto current_page_ptr = seastar::make_lw_shared<page>(first);
        return first.write(is).then([this, &is, current_page_ptr] {
            return seastar::repeat([this, &is, current_page_ptr]() mutable {
                auto next_page = current_page_ptr->get_next_page();
                if (is.size() == 0) {
                    current_page_ptr->set_next_page(null_page);
                    return current_page_ptr->flush(_file).then([this, next_page] {
                        if (next_page == null_page) {
                            return seastar::now();
                        }
                        return unlink_pages_from(next_page);
                    }).then([] {
                        return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                    });
                }
                return seastar::futurize<page>::invoke([this, next_page]() mutable {
                    if (next_page == null_page) {
                        return get_free_page();
                    }
                    return get_or_create_page(next_page);
                }).then([this, &is, current_page_ptr](auto next_page) mutable {
                    current_page_ptr->set_next_page(next_page.get_id());
                    return current_page_ptr->flush(_file).then([&is, current_page_ptr, next_page]() mutable {
                        next_page.set_type(page_type::overflow);
                        return next_page.write(is).then([current_page_ptr, next_page] {
                            *current_page_ptr = next_page;
                        });
                    });
                }).then([] {
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                });
            });
        });
    });
}

seastar::future<string> file_impl::read(page first) {
    seastar::temporary_buffer<char> buffer{first.get_record_length()};
    seastar::simple_memory_output_stream os{buffer.get_write(), buffer.size()};
    return seastar::do_with(os, [this, first](auto& os) mutable {
        auto current_page_ptr = seastar::make_lw_shared<page>(first);
        return seastar::repeat([this, &os, current_page_ptr]() mutable {
            return current_page_ptr->read(os).then([this, current_page_ptr]() mutable {
                if (current_page_ptr->get_next_page() == null_page) {
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                }
                return get_or_create_page(current_page_ptr->get_next_page()).then([current_page_ptr](auto next_page) mutable {
                    *current_page_ptr = next_page;
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                });
            });
        });
    }).then([buffer{buffer.share()}]() mutable {
        return seastar::make_ready_future<string>(std::move(buffer));
    });
}

seastar::future<> file_impl::unlink_pages_from(page first) {
    if (_header._first_free_page == null_page) {
        _header._first_free_page = first.get_id();
        _header._dirty = true;
    }
    return seastar::futurize<void>::invoke([this, first] {
        if (_header._last_free_page == null_page) {
            return seastar::now();
        }
        return get_or_create_page(_header._last_free_page).then([this, first](auto last_free_page) {
            last_free_page.set_next_page(first.get_id());
            return last_free_page.flush(_file).finally([last_free_page] {});
        });
    }).then([this, first] {
        auto current_page_ptr = seastar::make_lw_shared<page>(first);
        return seastar::repeat([this, current_page_ptr]() mutable {
            if (current_page_ptr->get_next_page() == null_page) {
                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
            }
            return get_or_create_page(current_page_ptr->get_next_page()).then([current_page_ptr](auto next_page) mutable {
                *current_page_ptr = next_page;
                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
            });
        }).then([this, current_page_ptr] {
            _header._last_free_page = current_page_ptr->get_id();
            _header._dirty = true;
        });
    });
}

file::file(std::string name, file_config config) {
    _impl = seastar::make_lw_shared<file_impl>(std::move(name), config);
}

file::file(const file& other_file) {
    _impl = other_file._impl;
}

file::file(file&& other_file) noexcept {
    _impl = std::move(other_file._impl);
}

file& file::operator=(const file& other_file) {
    _impl = other_file._impl;
    return *this;
}

file& file::operator=(file&& other_file) noexcept {
    _impl = std::move(other_file._impl);
    return *this;
}

seastar::future<> file::open() const {
    if (!_impl) {
        return seastar::now();
    }
    return _impl->open();
}

seastar::future<> file::close() const {
    if (!_impl || !_impl->is_open()) {
        SPIDERDB_LOGGER_WARN("File already closed");
        return seastar::now();
    }
    return _impl->close();
}

seastar::future<> file::write(string data) const {
    if (!_impl || !_impl->is_open()) {
        SPIDERDB_LOGGER_WARN("File already closed");
        return seastar::now();
    }
    if (data.empty()) {
        SPIDERDB_LOGGER_WARN("Empty string");
        return seastar::now();
    }
    return _impl->write(std::move(data));
}

seastar::future<string> file::read() const {
    if (!_impl || !_impl->is_open()) {
        SPIDERDB_LOGGER_WARN("File already closed");
        return seastar::make_ready_future<string>();
    }
    return _impl->read();
}

void file::log() const noexcept {
    if (!_impl || !_impl->is_open()) {
        SPIDERDB_LOGGER_WARN("File already closed");
        return;
    }
    return _impl->log();
}

}