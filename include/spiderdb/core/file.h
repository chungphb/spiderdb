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
    virtual seastar::future<> write(seastar::temporary_buffer<char> buffer);
    virtual seastar::future<> read(seastar::temporary_buffer<char> buffer);
    static constexpr size_t size() noexcept {
        return sizeof(_page_size) + sizeof(_page_count) + sizeof(_first_free_page) + sizeof(_last_free_page);
    }
    friend file_impl;

protected:
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
    file_impl(std::string name, spiderdb_config config);
    ~file_impl();
    virtual seastar::future<> open();
    virtual seastar::future<> flush();
    virtual seastar::future<> close();
    seastar::future<page_id> write(string data);
    seastar::future<> write(page_id id, string data);
    seastar::future<string> read(page_id id);
    seastar::future<> unlink_pages_from(page_id id);
    seastar::future<> write(page first, string data);
    seastar::future<string> read(page first);
    seastar::future<> unlink_pages_from(page first);
    virtual void log() const noexcept;
    virtual bool is_open() const noexcept;
    friend file;

protected:
    virtual seastar::shared_ptr<file_header> get_new_file_header();
    virtual seastar::shared_ptr<page_header> get_new_page_header();
    seastar::future<page> get_free_page();
    seastar::future<page> get_or_create_page(page_id id);

public:
    spiderdb_config _config;

protected:
    seastar::shared_ptr<file_header> _file_header = nullptr;

private:
    const std::string _name;
    seastar::file _file;
    std::unordered_map<page_id, seastar::weak_ptr<page_impl>> _pages;
    seastar::semaphore _file_lock{1};
    seastar::semaphore _get_free_page_lock{1};
};

struct file {
public:
    file() = delete;
    file(std::string name, spiderdb_config config = spiderdb_config());
    ~file() = default;
    file(const file& other);
    file(file&& other) noexcept;
    file& operator=(const file& other);
    file& operator=(file&& other) noexcept;
    const spiderdb_config& get_config() const;
    seastar::future<> open() const;
    seastar::future<> close() const;
    seastar::future<page_id> write(string data) const;
    seastar::future<string> read(page_id id) const;
    void log() const;

private:
    seastar::lw_shared_ptr<file_impl> _impl;
};

}