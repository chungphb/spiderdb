//
// Created by chungphb on 26/4/21.
//

#include <spiderdb/core/page.h>
#include <spiderdb/util/log.h>

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

page_impl::page_impl(page_id id, const file_config& config) : _id{id}, _config{config} {
    _data = std::move(string{_config.page_size, 0});
}

seastar::future<> page_impl::load(seastar::file file) {
    if (!file) {
        return seastar::now();
    }
    return seastar::with_semaphore(_lock, 1, [this, file]() mutable {
        const auto page_offset = _config.file_header_size + _id * _config.page_size;
        return file.dma_read_exactly<char>(page_offset, _config.page_size).then([this](auto buffer) {
            memcpy(_data.str(), buffer.get(), buffer.size());
            return _header.write(std::move(buffer));
        }).then([this] {
            SPIDERDB_LOGGER_DEBUG("Page {:0>12} - Loaded", _id);
            log();
        }).handle_exception([this](auto ex) {
            SPIDERDB_LOGGER_DEBUG("Page {:0>12} - Failed to load", _id);
        });
    });
}

seastar::future<> page_impl::flush(seastar::file file) {
    if (!file) {
        return seastar::now();
    }
    return seastar::with_semaphore(_lock, 1, [this, file]() mutable {
        seastar::temporary_buffer<char> buffer{_data.str(), _data.size()};
        memset(buffer.get_write(), 'h', _config.page_header_size);
        return _header.read(buffer.share()).then([this, file, buffer{buffer.share()}]() mutable {
            const auto page_offset = _config.file_header_size + _id * _config.page_size;
            return file.dma_write(page_offset, buffer.get_write(), buffer.size()).then([buffer{buffer.share()}](auto) {});
        }).then([this] {
            SPIDERDB_LOGGER_DEBUG("Page {:0>12} - Flushed", _id);
            log();
        }).handle_exception([this](auto ex) {
            SPIDERDB_LOGGER_DEBUG("Page {:0>12} - Failed to flush", _id);
        });
    });
}

seastar::future<> page_impl::write(seastar::simple_memory_input_stream& is) {
    return seastar::with_lock(_rwlock.for_write(), [this, &is] {
        _header._data_len = std::min(_config.page_size - _config.page_header_size, static_cast<uint32_t>(is.size()));
        if (_header._data_len > 0) {
            is.read(_data.str() + _config.page_header_size, _header._data_len);
        }
        return seastar::now();
    });
}

seastar::future<> page_impl::read(seastar::simple_memory_output_stream& os) {
    return seastar::with_lock(_rwlock.for_read(), [this, &os] {
        if (_header._data_len > 0) {
            os.write(_data.str() + _config.page_header_size, _header._data_len);
        }
        return seastar::now();
    });
}

void page_impl::log() const noexcept {
    SPIDERDB_LOGGER_TRACE("\t{:<18}{:>20}", "Type: ", _header._type);
    SPIDERDB_LOGGER_TRACE("\t{:<18}{:>20}", "Data length: ", _header._data_len);
    SPIDERDB_LOGGER_TRACE("\t{:<18}{:>20}", "Record length: ", _header._record_len);
    SPIDERDB_LOGGER_TRACE("\t{:<18}{:>20}", "Next page: ", _header._next);
}

page::page(page_id id, const file_config& config) {
    _impl = seastar::make_lw_shared<page_impl>(id, config);
}

page::page(seastar::lw_shared_ptr<page_impl> impl) {
    _impl = impl;
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

page_id page::get_id() const noexcept {
    if (!_impl) {
        return null_page;
    }
    return _impl->_id;
}

seastar::weak_ptr<page_impl> page::get_pointer() const noexcept {
    if (!_impl) {
        return nullptr;
    }
    return _impl->weak_from_this();
}

uint32_t page::get_record_length() const noexcept {
    if (!_impl) {
        return 0;
    }
    return _impl->_header._record_len;
}

page_id page::get_next_page() const noexcept {
    if (!_impl) {
        return null_page;
    }
    return _impl->_header._next;
}

page_type page::get_type() const noexcept {
    if (!_impl) {
        return page_type::unused;
    }
    return _impl->_header._type;
}

void page::set_record_length(uint32_t record_len) noexcept {
    if (!_impl) {
        return;
    }
    _impl->_header._record_len = record_len;
}

void page::set_next_page(page_id next) noexcept {
    if (!_impl) {
        return;
    }
    _impl->_header._next = next;
}

void page::set_type(page_type type) noexcept {
    if (!_impl) {
        return;
    }
    _impl->_header._type = type;
}

seastar::future<> page::load(seastar::file file) {
    if (!_impl) {
        return seastar::now();
    }
    return _impl->load(file);
}

seastar::future<> page::flush(seastar::file file) {
    if (!_impl) {
        return seastar::now();
    }
    return _impl->flush(file);
}

seastar::future<> page::write(seastar::simple_memory_input_stream& is) {
    if (!_impl) {
        return seastar::now();
    }
    return _impl->write(is);
}

seastar::future<> page::read(seastar::simple_memory_output_stream& os) {
    if (!_impl) {
        return seastar::now();
    }
    return _impl->read(os);
}

void page::log() const noexcept {
    if (!_impl) {
        return;
    }
    return _impl->log();
}

}