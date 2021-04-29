//
// Created by chungphb on 26/4/21.
//

#pragma once

#include <spiderdb/util/string.h>
#include <spiderdb/core/config.h>
#include <spiderdb/util/data_types.h>
#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/file.hh>

namespace spiderdb {

struct page_impl;
struct page;

struct page_header {
public:
    seastar::future<> write(seastar::temporary_buffer<char> buffer);
    seastar::future<> read(seastar::temporary_buffer<char> buffer);
    friend page_impl;
    friend page;

private:
    page_type _type = page_type::unused;
    uint32_t _data_len = 0;
    uint32_t _record_len = 0;
    page_id _next = null_page;
};

struct page_impl : seastar::enable_lw_shared_from_this<page_impl>, seastar::weakly_referencable<page_impl> {
public:
    page_impl() = delete;
    page_impl(page_id id, const file_config& config);
    ~page_impl() = default;
    seastar::future<> load(seastar::file file);
    seastar::future<> flush(seastar::file file);
    seastar::future<> write(seastar::simple_memory_input_stream& is);
    seastar::future<> read(seastar::simple_memory_output_stream& os);
    void log() const noexcept;
    friend page;

private:
    const page_id _id = null_page;
    const file_config& _config;
    page_header _header;
    string _data;
    seastar::semaphore _lock{1};
    seastar::rwlock _rwlock;
};

struct page {
public:
    page() = delete;
    page(page_id id, const file_config& config);
    page(seastar::lw_shared_ptr<page_impl> impl);
    page(const page& other_page);
    page(page&& other_page) noexcept;
    page& operator=(const page& other_page);
    page& operator=(page&& other_page) noexcept;
    page_id get_id() const noexcept;
    seastar::weak_ptr<page_impl> get_pointer() const noexcept;
    uint32_t get_record_length() const noexcept;
    page_id get_next_page() const noexcept;
    page_type get_type() const noexcept;
    void set_record_length(uint32_t record_len) noexcept;
    void set_next_page(page_id next) noexcept;
    void set_type(page_type type) noexcept;
    seastar::future<> load(seastar::file file);
    seastar::future<> flush(seastar::file file);
    seastar::future<> write(seastar::simple_memory_input_stream& is);
    seastar::future<> read(seastar::simple_memory_output_stream& os);
    void log() const noexcept;

private:
    seastar::lw_shared_ptr<page_impl> _impl;
};

}