//
// Created by chungphb on 4/5/21.
//

#pragma once

#include <spiderdb/core/node.h>
#include <spiderdb/core/file.h>
#include <spiderdb/util/cache.h>
#include <seastar/core/shared_future.hh>

namespace spiderdb {

struct btree_impl;
struct btree;

struct btree_header : file_header {
public:
    seastar::future<> write(seastar::temporary_buffer<char> buffer) override;
    seastar::future<> read(seastar::temporary_buffer<char> buffer) override;
    static constexpr size_t size() noexcept {
        return file_header::size() + sizeof(_root);
    }
    friend btree_impl;

protected:
    node_id _root = root_node;
};

struct btree_impl : file_impl, seastar::weakly_referencable<btree_impl> {
public:
    btree_impl() = delete;
    btree_impl(std::string name, spiderdb_config config);
    ~btree_impl() = default;
    node get_root() const noexcept;
    virtual seastar::future<> open() override;
    virtual seastar::future<> flush() override;
    virtual seastar::future<> close() override;
    seastar::future<> add(string&& key, value_pointer ptr);
    seastar::future<value_pointer> remove(string&& key);
    seastar::future<value_pointer> find(string&& key);
    seastar::future<node> create_node(node_type type, seastar::weak_ptr<node_impl>&& parent = nullptr);
    seastar::future<node> get_node(node_id id, seastar::weak_ptr<node_impl>&& parent = nullptr);
    seastar::future<> cache_node(node node);
    virtual void log() const noexcept override;
    virtual bool is_open() const noexcept override;
    friend btree;

protected:
    seastar::shared_ptr<file_header> get_new_file_header() override;
    seastar::shared_ptr<page_header> get_new_page_header() override;

private:
    seastar::weak_ptr<btree_impl> get_pointer() noexcept;

protected:
    seastar::shared_ptr<btree_header> _btree_header = nullptr;

private:
    node _root;
    std::unique_ptr<cache<node_id, node>> _cache;
    std::unordered_map<node_id, seastar::weak_ptr<node_impl>> _nodes;
    seastar::semaphore _get_node_lock{1};
};

struct btree {
public:
    btree() = delete;
    btree(std::string name, spiderdb_config config = spiderdb_config());
    ~btree() = default;
    btree(const btree& other);
    btree(btree&& other) noexcept;
    btree& operator=(const btree& other);
    btree& operator=(btree&& other) noexcept;
    const spiderdb_config& get_config() const;
    seastar::future<> open() const;
    seastar::future<> close() const;
    seastar::future<> add(string&& key, value_pointer ptr) const;
    seastar::future<value_pointer> remove(string&& key) const;
    seastar::future<value_pointer> find(string&& key) const;
    void log() const;

private:
    seastar::lw_shared_ptr<btree_impl> _impl;
};

}