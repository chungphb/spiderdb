//
// Created by chungphb on 26/4/21.
//

#include <spiderdb/core/page.h>
#include <spiderdb/util/log.h>
#include <spiderdb/util/error.h>

namespace spiderdb {

seastar::future<> page_header::write(seastar::temporary_buffer<char> buffer) {
    memcpy(&_type, buffer.begin(), sizeof(_type));
    buffer.trim_front(sizeof(_type));
    memcpy(&_data_len, buffer.begin(), sizeof(_data_len));
    buffer.trim_front(sizeof(_data_len));
    memcpy(&_record_len, buffer.begin(), sizeof(_record_len));
    buffer.trim_front(sizeof(_record_len));
    memcpy(&_next, buffer.begin(), sizeof(_next));
    buffer.trim_front(sizeof(_next));
    return seastar::now();
}

seastar::future<> page_header::read(seastar::temporary_buffer<char> buffer) {
    memcpy(buffer.get_write(), &_type, sizeof(_type));
    buffer.trim_front(sizeof(_type));
    memcpy(buffer.get_write(), &_data_len, sizeof(_data_len));
    buffer.trim_front(sizeof(_data_len));
    memcpy(buffer.get_write(), &_record_len, sizeof(_record_len));
    buffer.trim_front(sizeof(_record_len));
    memcpy(buffer.get_write(), &_next, sizeof(_next));
    buffer.trim_front(sizeof(_next));
    return seastar::now();
}

void page_header::log() const noexcept {
    SPIDERDB_LOGGER_TRACE("\t{:<18}{:>20}", "Type: ", page_type_to_string(_type));
    SPIDERDB_LOGGER_TRACE("\t{:<18}{:>20}", "Data length: ", _data_len);
    SPIDERDB_LOGGER_TRACE("\t{:<18}{:>20}", "Record length: ", _record_len);
    SPIDERDB_LOGGER_TRACE("\t{:<18}{:>20}", "Next page: ", _next);
}

page_impl::page_impl(page_id id, const spiderdb_config& config) : _id{id}, _config{config} {
    _data = string{_config.page_size, 0};
}

uint32_t page_impl::get_work_size() const noexcept {
    return _config.page_size - _config.page_header_size;
}

seastar::future<> page_impl::load(seastar::file file) {
    if (!file) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::closed_error});
    }
    if (!is_valid()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::page_unavailable});
    }
    return seastar::with_semaphore(_lock, 1, [this, file]() mutable {
        const auto page_offset = _config.file_header_size + _id.get() * _config.page_size;
        return file.dma_read_exactly<char>(page_offset, _config.page_size).then([this](auto buffer) {
            memcpy(_data.str(), buffer.get(), buffer.size());
            return _header->write(std::move(buffer));
        }).then([this] {
            if (_header->_type == page_type::internal || _header->_type == page_type::leaf) {
                SPIDERDB_LOGGER_DEBUG("Node {:0>12} - Loaded", _id);
            } else {
                SPIDERDB_LOGGER_DEBUG("Page {:0>12} - Loaded", _id);
            }
            _header->log();
        }).handle_exception([this](auto ex) {
            try {
                std::rethrow_exception(ex);
            } catch (seastar::file::eof_error& err) {
            } catch (...) {
                if (_header->_type == page_type::internal || _header->_type == page_type::leaf) {
                    SPIDERDB_LOGGER_DEBUG("Node {:0>12} - Failed to load", _id);
                } else {
                    SPIDERDB_LOGGER_DEBUG("Page {:0>12} - Failed to load", _id);
                }
            }
        });
    });
}

seastar::future<> page_impl::flush(seastar::file file) {
    if (!file) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::closed_error});
    }
    if (!is_valid()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::page_unavailable});
    }
    return seastar::with_semaphore(_lock, 1, [this, file]() mutable {
        seastar::temporary_buffer<char> buffer{_data.str(), _data.size()};
        memset(buffer.get_write(), 0, _config.page_header_size);
        return _header->read(buffer.share()).then([this, file, buffer{buffer.share()}]() mutable {
            const auto page_offset = _config.file_header_size + _id.get() * _config.page_size;
            return file.dma_write(page_offset, buffer.get_write(), buffer.size()).then([buffer{buffer.share()}](auto) {});
        }).then([this] {
            if (_header->_type == page_type::internal || _header->_type == page_type::leaf) {
                SPIDERDB_LOGGER_DEBUG("Node {:0>12} - Flushed", _id);
            } else {
                SPIDERDB_LOGGER_DEBUG("Page {:0>12} - Flushed", _id);
            }
            _header->log();
        }).handle_exception([this](auto ex) {
            if (_header->_type == page_type::internal || _header->_type == page_type::leaf) {
                SPIDERDB_LOGGER_DEBUG("Node {:0>12} - Failed to flush", _id);
            } else {
                SPIDERDB_LOGGER_DEBUG("Page {:0>12} - Failed to flush", _id);
            }
        });
    });
}

seastar::future<> page_impl::write(seastar::simple_memory_input_stream& is) {
    if (!is_valid()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::page_unavailable});
    }
    return seastar::with_lock(_rwlock.for_write(), [this, &is] {
        _header->_data_len = std::min(get_work_size(), static_cast<uint32_t>(is.size()));
        if (_header->_data_len > 0) {
            is.read(_data.str() + _config.page_header_size, _header->_data_len);
        }
        return seastar::now();
    });
}

seastar::future<> page_impl::read(seastar::simple_memory_output_stream& os) {
    if (!is_valid()) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::page_unavailable});
    }
    return seastar::with_lock(_rwlock.for_read(), [this, &os] {
        if (_header->_data_len > 0) {
            os.write(_data.str() + _config.page_header_size, _header->_data_len);
        }
        return seastar::now();
    });
}

void page_impl::log() const noexcept {
    _header->log();
}

bool page_impl::is_valid() const noexcept {
    return (bool)_header;
}

page::page(page_id id, const spiderdb_config& config) {
    _impl = seastar::make_lw_shared<page_impl>(id, config);
}

page::page(seastar::lw_shared_ptr<page_impl> impl) {
    _impl = std::move(impl);
}

page::page(const page& other_page) {
    _impl = other_page._impl;
}

page::page(page&& other_page) noexcept {
    _impl = std::move(other_page._impl);
}

page& page::operator=(const page& other_page) {
    _impl = other_page._impl;
    return *this;
}

page& page::operator=(page&& other_page) noexcept {
    _impl = std::move(other_page._impl);
    return *this;
}

page_id page::get_id() const {
    if (!_impl) {
        throw spiderdb_error{error_code::page_unavailable};
    }
    return _impl->_id;
}

seastar::weak_ptr<page_impl> page::get_pointer() const {
    if (!_impl) {
        throw spiderdb_error{error_code::page_unavailable};
    }
    return _impl->weak_from_this();
}

seastar::shared_ptr<page_header> page::get_header() const {
    if (!_impl) {
        throw spiderdb_error{error_code::page_unavailable};
    }
    return _impl->_header;
}

uint32_t page::get_work_size() const {
    if (!_impl) {
        throw spiderdb_error{error_code::page_unavailable};
    }
    return _impl->get_work_size();
}

uint32_t page::get_record_length() const {
    if (!_impl) {
        throw spiderdb_error{error_code::page_unavailable};
    }
    return _impl->_header->_record_len;
}

page_id page::get_next_page() const {
    if (!_impl) {
        throw spiderdb_error{error_code::page_unavailable};
    }
    return _impl->_header->_next;
}

page_type page::get_type() const {
    if (!_impl) {
        throw spiderdb_error{error_code::page_unavailable};
    }
    return _impl->_header->_type;
}

void page::set_header(seastar::shared_ptr<page_header> header) {
    if (!_impl) {
        throw spiderdb_error{error_code::page_unavailable};
    }
    _impl->_header = std::move(header);
}

void page::set_record_length(uint32_t record_len) {
    if (!_impl) {
        throw spiderdb_error{error_code::page_unavailable};
    }
    _impl->_header->_record_len = record_len;
}

void page::set_next_page(page_id next) {
    if (!_impl) {
        throw spiderdb_error{error_code::page_unavailable};
    }
    _impl->_header->_next = next;
}

void page::set_type(page_type type) {
    if (!_impl) {
        throw spiderdb_error{error_code::page_unavailable};
    }
    _impl->_header->_type = type;
}

seastar::future<> page::load(seastar::file file) {
    if (!_impl) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::page_unavailable});
    }
    return _impl->load(std::move(file));
}

seastar::future<> page::flush(seastar::file file) {
    if (!_impl) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::page_unavailable});
    }
    return _impl->flush(std::move(file));
}

seastar::future<> page::write(seastar::simple_memory_input_stream& is) {
    if (!_impl) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::page_unavailable});
    }
    return _impl->write(is);
}

seastar::future<> page::read(seastar::simple_memory_output_stream& os) {
    if (!_impl) {
        return seastar::make_exception_future<>(spiderdb_error{error_code::page_unavailable});
    }
    return _impl->read(os);
}

void page::log() const {
    if (!_impl) {
        throw spiderdb_error{error_code::page_unavailable};
    }
    _impl->log();
}

}