//
// Created by chungphb on 3/5/21.
//

#pragma once

#include <spiderdb/core/page.h>

namespace spiderdb {

struct node_impl;
struct node;
struct btree_impl;

struct node_header : page_header {
public:
    seastar::future<> write(seastar::temporary_buffer<char> buffer) override;
    seastar::future<> read(seastar::temporary_buffer<char> buffer) override;
    void log() const noexcept override;
    static constexpr size_t size() noexcept {
        return page_header::size() + sizeof(_parent) + sizeof(_key_count) + sizeof(_prefix_len);
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
    seastar::future<> load();
    seastar::future<> flush();
    seastar::future<> add(string&& key, value_pointer ptr);
    seastar::future<value_pointer> remove(string&& key);
    seastar::future<value_pointer> find(string&& key);
    seastar::future<node> get_parent();
    seastar::future<node> get_child(uint32_t id);
    void update_parent(seastar::weak_ptr<node_impl>&& parent) noexcept;
    int64_t binary_search(const string& key, int64_t low, int64_t high);
    seastar::future<> split();
    bool need_split() noexcept;
    seastar::future<> promote(string&& promoted_key, node_id left_child, node_id right_child);
    seastar::future<> merge();
    bool need_merge() noexcept;
    seastar::future<string> demote(node_id left_child, node_id right_child);
    seastar::future<> destroy();
    bool need_destroy() noexcept;
    seastar::future<> fire(node_id child);
    void log() const;
    friend node;

private:
    seastar::future<node> create_node(std::vector<string>&& keys, std::vector<node_item_pointer>&& pointers);
    seastar::future<> link_siblings(node left, node right);
    seastar::future<> cache(node node);
    seastar::future<> become_parent();
    void update_data(std::vector<string>&& keys, std::vector<node_item_pointer>&& pointers);
    void update_metadata();
    void calculate_data_length() noexcept;
    seastar::future<> clean();
    bool is_valid() const noexcept;

private:
    const node_id _id = null_node;
    page _page;
    seastar::weak_ptr<btree_impl> _btree;
    seastar::shared_ptr<node_header> _header;
    std::vector<string> _keys;
    std::vector<node_item_pointer> _pointers;
    seastar::weak_ptr<node_impl> _parent;
    node_id _next = null_node;
    node_id _prev = null_node;
    string _prefix;
    string _high_key;
    size_t _data_len = 0;
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
    explicit operator bool() const noexcept;
    bool operator!() const noexcept;

    // Getters and setters
    node_id get_id() const;
    seastar::weak_ptr<node_impl> get_pointer() const;
    page get_page() const;
    const std::vector<string>& get_key_list() const;
    const std::vector<node_item_pointer>& get_pointer_list() const;
    node_id get_parent_node() const;
    node_id get_next_node() const;
    node_id get_prev_node() const;
    const string& get_high_key() const;
    void set_next_node(node_id next) const;
    void set_prev_node(node_id prev) const;
    void set_high_key(string&& high_key) const;
    void mark_dirty() const;

    // APIs
    seastar::future<> load() const;
    seastar::future<> flush() const;
    seastar::future<> add(string&& key, value_pointer ptr) const;
    seastar::future<value_pointer> remove(string&& key) const;
    seastar::future<value_pointer> find(string&& key) const;
    void update_parent(seastar::weak_ptr<node_impl>&& parent) const;
    int64_t binary_search(const string& key, int64_t low, int64_t high) const;
    seastar::future<> split() const;
    bool need_split() const;
    seastar::future<> promote(string&& key, node_id left_child, node_id right_child) const;
    seastar::future<> merge() const;
    bool need_merge() const;
    seastar::future<string> demote(node_id left_child, node_id right_child) const;
    seastar::future<> destroy() const;
    bool need_destroy() const;
    seastar::future<> fire(node_id child) const;
    seastar::future<> become_parent() const;
    void update_data(std::vector<string>&& keys, std::vector<node_item_pointer>&& pointers) const;
    void update_metadata() const;
    seastar::future<> clean() const;
    void log() const;

private:
    seastar::lw_shared_ptr<node_impl> _impl;
};

}