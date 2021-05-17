//
// Created by chungphb on 15/5/21.
//

#pragma once

#include <spiderdb/core/node.h>

namespace spiderdb {

struct data_page_impl;
struct data_page;
struct storage_impl;

struct data_page_header : node_header {
public:
    seastar::future<> write(seastar::temporary_buffer<char> buffer) override;
    seastar::future<> read(seastar::temporary_buffer<char> buffer) override;
    friend data_page_impl;
    friend data_page;

private:
    uint32_t _value_count = 0;
};

struct data_page_impl : seastar::enable_lw_shared_from_this<data_page_impl>, seastar::weakly_referencable<data_page_impl> {
public:
    data_page_impl() = delete;
    data_page_impl(page page, seastar::weak_ptr<storage_impl>&& storage);
    ~data_page_impl() = default;
    seastar::future<> load();
    seastar::future<> flush();
    seastar::future<value_id> add(string&& value);
    seastar::future<> update(value_id id, string&& value);
    seastar::future<> remove(value_id id);
    seastar::future<string> find(value_id id);
    void log() const;
    friend data_page;

private:
    seastar::future<> cache(data_page data_page);
    seastar::future<> clean();
    bool is_valid() const noexcept;

private:
    page _page;
    seastar::weak_ptr<storage_impl> _storage;
    seastar::shared_ptr<data_page_header> _header;
    std::vector<string> _values;
    size_t _data_len = 0;
    seastar::rwlock _rwlock;
    bool _loaded = false;
    bool _dirty = false;
};

struct data_page {
public:
    data_page() = default;
    data_page(page page, seastar::weak_ptr<storage_impl>&& storage);
    data_page(seastar::lw_shared_ptr<data_page_impl> impl);
    ~data_page() = default;
    data_page(const data_page& other_data_page);
    data_page(data_page&& other_data_page) noexcept;
    data_page& operator=(const data_page& other_data_page);
    data_page& operator=(data_page&& other_data_page) noexcept;
    explicit operator bool() const noexcept;
    bool operator!() const noexcept;

    // Getters and setters
    page_id get_id() const;
    seastar::weak_ptr<data_page_impl> get_pointer() const;
    page get_page() const;
    const std::vector<string>& get_value_list() const;
    size_t get_data_length() const;
    void mark_dirty() const;

    // APIs
    seastar::future<> load() const;
    seastar::future<> flush() const;
    seastar::future<value_id> add(string&& value) const;
    seastar::future<> update(value_id id, string&& value) const;
    seastar::future<> remove(value_id id) const;
    seastar::future<string> find(value_id id) const;
    void log() const;

private:
    seastar::lw_shared_ptr<data_page_impl> _impl;
};

}