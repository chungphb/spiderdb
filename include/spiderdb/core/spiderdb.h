//
// Created by chungphb on 6/19/21.
//

#pragma once

#include <spiderdb/core/storage.h>
#include <seastar/core/distributed.hh>

namespace spiderdb {

struct spiderdb;

struct spiderdb_impl {
public:
    spiderdb_impl() = delete;
    spiderdb_impl(std::string name, spiderdb_config config);
    ~spiderdb_impl() = default;
    seastar::future<> open();
    seastar::future<> flush();
    seastar::future<> close();
    seastar::future<> insert(string&& key, string&& value);
    seastar::future<> update(string&& key, string&& value);
    seastar::future<> erase(string&& key);
    seastar::future<string> select(string&& key);
    bool is_open() const noexcept;
    friend struct spiderdb;

private:
    std::string _name;
    spiderdb_config _config;
    seastar::distributed<storage_impl> _storage;
};

struct spiderdb {
public:
    spiderdb() = delete;
    spiderdb(std::string name, spiderdb_config config = spiderdb_config());
    ~spiderdb() = default;
    spiderdb(const spiderdb& other);
    spiderdb(spiderdb&& other) noexcept;
    spiderdb& operator=(const spiderdb& other);
    spiderdb& operator=(spiderdb&& other) noexcept;
    const spiderdb_config& get_config() const;
    seastar::future<> open() const;
    seastar::future<> close() const;
    seastar::future<> insert(string&& key, string&& value) const;
    seastar::future<> update(string&& key, string&& value) const;
    seastar::future<> erase(string&& key) const;
    seastar::future<string> select(string&& key) const;

private:
    seastar::lw_shared_ptr<spiderdb_impl> _impl;
};

}