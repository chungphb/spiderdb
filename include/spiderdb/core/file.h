//
// Created by chungphb on 27/4/21.
//

#pragma once

#include <spiderdb/core/page.h>
#include <spiderdb/core/config.h>
#include <seastar/core/file.hh>
#include <seastar/core/semaphore.hh>
#include <chrono>

namespace spiderdb {

struct file_impl;
struct file;

struct file_header {
public:
    seastar::future<> flush(seastar::file file);
    seastar::future<> load(seastar::file file);
    seastar::future<> write(seastar::temporary_buffer<char> buffer);
    seastar::future<> read(seastar::temporary_buffer<char> buffer);
    static constexpr size_t size() noexcept {
        return sizeof(_page_size) + sizeof(_page_count) + sizeof(_first_free_page) + sizeof(_last_free_page);
    }
    friend file_impl;

private:
    uint16_t _size = 0;
    uint32_t _page_size = 0;
    uint64_t _page_count = 0;
    page_id _first_free_page = null_page;
    page_id _last_free_page = null_page;
    bool _dirty = true;
};

struct file_impl : seastar::weakly_referencable<file_impl> {
public:
    file_impl() = delete;
    file_impl(std::string name, file_config config);
    ~file_impl();
    seastar::future<> open();
    seastar::future<> flush();
    seastar::future<> close();
    seastar::future<page_id> write(string data);
    seastar::future<> write(page_id id, string data);
    seastar::future<string> read(page_id id);
    void log() const noexcept;
    bool is_open() const noexcept;
    friend file;

private:
    seastar::future<page> get_free_page();
    seastar::future<page> get_or_create_page(page_id id);
    seastar::future<> unlink_pages_from(page_id id);
    seastar::future<> write(page first, string data);
    seastar::future<string> read(page first);
    seastar::future<> unlink_pages_from(page first);
    void increase_page_count() noexcept;

private:
    const std::string _name;
    seastar::file _file;
    file_config _config;
    file_header _header;
    std::unordered_map<page_id, seastar::weak_ptr<page_impl>> _pages;
    seastar::semaphore _lock{1};
    seastar::semaphore _get_free_page_lock{1};
};

struct node_impl;
struct btree_impl;

struct file {
public:
    file() = delete;
    file(std::string name, file_config config = file_config());
    ~file() = default;
    file(const file& other_file);
    file(file&& other_file) noexcept;
    file& operator=(const file& other_file);
    file& operator=(file&& other_file) noexcept;
    const file_config& get_config() const;
    seastar::future<> open() const;
    seastar::future<> close() const;
    seastar::future<page_id> write(string data) const;
    seastar::future<string> read(page_id id) const;
    void log() const noexcept;
    friend node_impl;
    friend btree_impl;

private:
    seastar::future<> flush() const;
    seastar::future<page> get_free_page() const;
    seastar::future<page> get_or_create_page(page_id id) const;
    seastar::future<> unlink_pages_from(page_id id) const;
    seastar::future<> write(page first, string data) const;
    seastar::future<string> read(page first) const;
    seastar::future<> unlink_pages_from(page first) const;
    void increase_page_count() const noexcept;

private:
    seastar::lw_shared_ptr<file_impl> _impl;
};

}