//
// Created by chungphb on 3/5/21.
//

#include <spiderdb/core/page.h>

#pragma once

namespace spiderdb {

struct node_impl;
struct node;
struct btree_impl;

struct node_header {
public:
    seastar::future<> write(seastar::temporary_buffer<char> buffer);
    seastar::future<> read(seastar::temporary_buffer<char> buffer);
    static constexpr size_t size() noexcept {
        return sizeof(_parent) + sizeof(_key_count) + sizeof(_prefix_len);
    }
    friend node_impl;
    friend node;

private:
    node_id _parent = null_node;
    uint32_t _key_count = 0;
    uint32_t _prefix_len = 0;
};

struct node_impl : seastar::enable_lw_shared_from_this<node_impl>, seastar::weakly_referencable<node_impl> {
public:
    node_impl() = delete;
    node_impl(page page, seastar::weak_ptr<btree_impl>&& btree, seastar::weak_ptr<node_impl>&& parent);
    ~node_impl() = default;
    void set_parent_node(seastar::weak_ptr<node_impl>&& parent) noexcept;
    seastar::future<> load();
    seastar::future<> flush();
    seastar::future<> add(string&& key, data_pointer ptr);
    seastar::future<> remove(string&& key);
    seastar::future<data_pointer> find(string&& key);
    seastar::future<node> get_parent();
    seastar::future<node> get_child(uint32_t id);
    int64_t binary_search(const string& key, uint32_t low, uint32_t high);
    seastar::future<> split();
    bool need_split() noexcept;
    seastar::future<> promote(string&& promoted_key, node_id left_child, node_id right_child);
    seastar::future<> merge();
    bool need_merge() noexcept;
    seastar::future<string> demote(node_id left_child, node_id right_child);
    void log() const noexcept;
    friend node;

private:
    seastar::future<node> create_node(std::vector<string>&& keys, std::vector<pointer>&& pointers);
    seastar::future<> link_siblings(node left, node right);
    seastar::future<> cache(node node);
    seastar::future<> become_parent();
    void update_data(std::vector<string>&& keys, std::vector<pointer>&& pointers) noexcept;
    void update_metadata() noexcept;
    size_t calculate_data_length(bool reset = false) noexcept;
    seastar::future<> invalidate();
    seastar::future<> clean();

private:
    page _page;
    seastar::weak_ptr<btree_impl> _btree;
    node_header _header;
    std::vector<string> _keys;
    std::vector<pointer> _pointers;
    seastar::weak_ptr<node_impl> _parent;
    node_id _next = null_node;
    node_id _prev = null_node;
    string _prefix;
    string _high_key;
    size_t _data_len;
    seastar::semaphore _lock{1};
    bool _loaded = false;
    bool _dirty = false;
};

struct node {
public:
    node() = default;
    node(page page, seastar::weak_ptr<btree_impl>&& btree, seastar::weak_ptr<node_impl>&& parent = nullptr);
    node(seastar::lw_shared_ptr<node_impl> impl);
    ~node() = default;
    node(const node& other_node);
    node(node&& other_node) noexcept;
    node& operator=(const node& other_node);
    node& operator=(node&& other_node) noexcept;
    operator bool() const noexcept;
    bool operator!() const noexcept;

    // Getters and setters
    node_id get_id() const noexcept;
    seastar::weak_ptr<node_impl> get_pointer() const noexcept;
    page get_page() const;
    const std::vector<string>& get_key_list() const;
    const std::vector<pointer>& get_pointer_list() const;
    node_id get_parent_node() const noexcept;
    node_id get_next_node() const noexcept;
    node_id get_prev_node() const noexcept;
    const string& get_high_key() const;
    void set_parent_node(seastar::weak_ptr<node_impl>&& parent) const noexcept;
    void set_next_node(node_id next) const noexcept;
    void set_prev_node(node_id prev) const noexcept;
    void set_high_key(string&& high_key) const noexcept;
    void mark_dirty() const noexcept;

    // APIs
    seastar::future<> load() const;
    seastar::future<> flush() const;
    seastar::future<> add(string&& key, data_pointer ptr) const;
    seastar::future<> remove(string&& key) const;
    seastar::future<data_pointer> find(string&& key) const;
    int32_t binary_search(const string& key, int32_t low, int32_t high) const;
    seastar::future<> split() const;
    bool need_split() const noexcept;
    seastar::future<> promote(string&& key, node_id left_child, node_id right_child) const;
    seastar::future<> merge() const;
    bool need_merge() const noexcept;
    seastar::future<string> demote(node_id left_child, node_id right_child) const;
    seastar::future<> become_parent();
    void update_data(std::vector<string>&& keys, std::vector<pointer>&& pointers) const noexcept;
    seastar::future<> clean();
    void log() const noexcept;

private:
    seastar::lw_shared_ptr<node_impl> _impl;
};

}