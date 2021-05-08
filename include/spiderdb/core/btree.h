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

struct btree_header {
public:
    seastar::future<> write(seastar::temporary_buffer<char> buffer);
    seastar::future<> read(seastar::temporary_buffer<char> buffer);
    static constexpr size_t size() noexcept {
        return sizeof(_root);
    }
    friend btree_impl;

protected:
    node_id _root = root_node;
};

struct btree_impl : seastar::weakly_referencable<btree_impl> {
public:
    btree_impl() = delete;
    btree_impl(std::string name, spiderdb_config config);
    ~btree_impl() = default;
    node get_root() const noexcept;
    file get_file() const noexcept;
    const btree_config& get_config() const noexcept;
    seastar::future<> open();
    seastar::future<> flush();
    seastar::future<> close();
    seastar::future<> add(string&& key, data_pointer ptr);
    seastar::future<> remove(string&& key);
    seastar::future<data_pointer> find(string&& key);
    seastar::future<node> create_node(node_type type, seastar::weak_ptr<node_impl>&& parent = nullptr);
    seastar::future<node> get_node(node_id id, seastar::weak_ptr<node_impl>&& parent = nullptr);
    seastar::future<> cache_node(node node);
    void log() const noexcept;
    bool is_open() const noexcept;
    friend btree;

private:
    node _root;
    file _file;
    btree_config _config;
    btree_header _header;
    std::unique_ptr<cache<node_id, node>> _cache;
    std::unordered_map<node_id, seastar::weak_ptr<node_impl>> _nodes;
    seastar::semaphore _get_node_lock{1};
};

struct btree {
public:
    btree() = delete;
    btree(std::string name, spiderdb_config config = spiderdb_config());
    ~btree() = default;
    btree(const btree& other_btree);
    btree(btree&& other_btree) noexcept;
    btree& operator=(const btree& other_btree);
    btree& operator=(btree&& other_btree) noexcept;
    const btree_config& get_config() const;
    seastar::future<> open() const;
    seastar::future<> close() const;
    seastar::future<> add(string&& key, data_pointer ptr) const;
    seastar::future<> remove(string&& key) const;
    seastar::future<data_pointer> find(string&& key) const;
    void log() const noexcept;

private:
    seastar::lw_shared_ptr<btree_impl> _impl;
};

}