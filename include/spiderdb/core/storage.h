//
// Created by chungphb on 15/5/21.
//

#pragma once

#include <spiderdb/core/data_page.h>
#include <spiderdb/core/btree.h>
#include <spiderdb/util/cache.h>
#include <seastar/core/shared_future.hh>

namespace spiderdb {

struct storage_impl;
struct storage;

struct available_page_list {
public:
    available_page_list() = delete;
    available_page_list(size_t capacity);
    ~available_page_list() = default;
    void set_min_available_space(uint32_t min_available_space);
    void add(page_id id, uint32_t available_space);
    void remove(page_id id);
    page_id find(uint32_t required_space);
    seastar::future<> write(seastar::temporary_buffer<char> buffer);
    seastar::future<> read(seastar::temporary_buffer<char> buffer);
    size_t size() const noexcept;

private:
    const uint64_t _capacity = 0;
    uint32_t _min_available_space = 0;
    std::unordered_map<page_id, uint32_t> _available_pages;
};

struct storage_header : btree_header {
public:
    seastar::future<> write(seastar::temporary_buffer<char> buffer) override;
    seastar::future<> read(seastar::temporary_buffer<char> buffer) override;
    friend storage_impl;

private:
    std::unique_ptr<available_page_list> _available_page_list;
};

struct storage_impl : btree_impl, seastar::weakly_referencable<storage_impl> {
public:
    storage_impl() = delete;
    storage_impl(std::string name, spiderdb_config config);
    ~storage_impl() = default;
    seastar::future<> open() override;
    seastar::future<> flush() override;
    seastar::future<> close() override;
    seastar::future<> insert(string&& key, string&& value);
    seastar::future<> update(string&& key, string&& value);
    seastar::future<> erase(string&& key);
    seastar::future<string> select(string&& key);
    void log() const noexcept override;
    bool is_open() const noexcept override;
    friend storage;

private:
    seastar::shared_ptr<file_header> get_new_file_header() override;
    seastar::shared_ptr<page_header> get_new_page_header() override;
    seastar::weak_ptr<storage_impl> get_pointer() noexcept;
    seastar::future<data_page> create_data_page();
    seastar::future<data_page> get_data_page(page_id id);
    seastar::future<> cache_data_page(data_page data_page);
    seastar::future<data_pointer> add_value(string&& value);
    seastar::future<> update_value(data_pointer ptr, string&& value);
    seastar::future<> remove_value(data_pointer ptr);
    seastar::future<string> find_value(data_pointer ptr);
    data_pointer generate_data_pointer(page_id pid, value_id vid);
    page_id get_page_id(data_pointer ptr);
    value_id get_value_id(data_pointer ptr);

private:
    seastar::shared_ptr<storage_header> _storage_header = nullptr;
    std::unique_ptr<cache<page_id, data_page>> _cache;
    std::unordered_map<page_id, seastar::weak_ptr<data_page_impl>> _data_pages;
    seastar::semaphore _create_data_page_lock{1};
    seastar::semaphore _get_data_page_lock{1};
};

struct storage {
public:
    storage() = delete;
    storage(std::string name, spiderdb_config config = spiderdb_config());
    ~storage() = default;
    storage(const storage& other_storage);
    storage(storage&& other_storage) noexcept;
    storage& operator=(const storage& other_storage);
    storage& operator=(storage&& other_storage) noexcept;
    const spiderdb_config& get_config() const;
    seastar::future<> open() const;
    seastar::future<> close() const;
    seastar::future<> insert(string&& key, string&& value) const;
    seastar::future<> update(string&& key, string&& value) const;
    seastar::future<> erase(string&& key) const;
    seastar::future<string> select(string&& key) const;
    void log() const;

private:
    seastar::lw_shared_ptr<storage_impl> _impl;
};

}