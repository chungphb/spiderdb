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

struct storage_header : btree_header {};

struct storage_impl : btree_impl, seastar::weakly_referencable<storage_impl> {};

struct storage {};

}