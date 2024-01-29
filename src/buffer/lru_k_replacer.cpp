//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
#include "fmt/format.h"

namespace bustub {

struct NodeInfo {
  frame_id_t fid_;
  bool is_inf_;
  time_t_ k_dis_;
  NodeInfo() = default;
  NodeInfo(frame_id_t fid, bool isInf, time_t_ kDis) : fid_(fid), is_inf_(isInf), k_dis_(kDis) {}
  auto operator<(const NodeInfo &other) const -> bool {
    if (is_inf_ != other.is_inf_) {
      return is_inf_;
    }
    if (is_inf_) {
      return k_dis_ < other.k_dis_;
    }
    return k_dis_ > other.k_dis_;
  }
};

struct Lockker {
  std::mutex &mutex_;
  explicit Lockker(std::mutex &mutex) : mutex_(mutex) {}
  void Lock() { mutex_.lock(); }
  virtual ~Lockker() { mutex_.unlock(); }
};

/**
 * 构造函数
 * @param num_frames 最多需要存储的页面数
 * @param k
 * */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  Lockker _(latch_);
  _.Lock();
  if (node_evict_.empty()) {
    return false;
  }
  bool is_inf;
  time_t_ k_dis;
  std::vector<NodeInfo> ve;
  for (const auto &[k, v] : node_evict_) {
    is_inf = v.CalcKDis(current_timestamp_, k_dis);
    ve.emplace_back(k, is_inf, k_dis);
  }

  std::sort(ve.begin(), ve.end());

  node_evict_.erase(ve[0].fid_);

  *frame_id = ve[0].fid_;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  Lockker _(latch_);
  _.Lock();
  if (node_evict_.count(frame_id) != 0U) {
    auto &node = node_evict_.at(frame_id);

    node.Access(current_timestamp_);
    current_timestamp_++;

    return;
  }
  if (node_store_.count(frame_id) != 0U) {
    auto &node = node_store_.at(frame_id);

    node.Access(current_timestamp_);
    current_timestamp_++;

    return;
  }

  auto tmp = LRUKNode(frame_id, k_);

  tmp.Access(current_timestamp_);
  current_timestamp_++;
  // 默认不可换出
  node_store_.emplace(frame_id, std::move(tmp));
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  Lockker _(latch_);
  _.Lock();
  if (node_evict_.count(frame_id) != 0U) {
    if (set_evictable) {
      return;
    }
    auto i_tmp = node_evict_.find(frame_id);
    node_store_.emplace(frame_id, std::move(i_tmp->second));
    node_evict_.erase(i_tmp);
    return;
  }

  if (node_store_.count(frame_id) != 0U) {
    if (not set_evictable) {
      return;
    }
    auto i_tmp = node_store_.find(frame_id);
    node_evict_.emplace(frame_id, std::move(i_tmp->second));
    node_store_.erase(i_tmp);
    return;
  }

  throw bustub::Exception(fmt::format("No matching pages@{}.", frame_id));
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  Lockker _(latch_);
  _.Lock();
  if (node_evict_.count(frame_id) != 0U) {
    node_evict_.erase(frame_id);
    return;
  }
  if (node_store_.count(frame_id) != 0U) {
    throw bustub::Exception(fmt::format("No page@{} is not Evictable.", frame_id));
  }
}

auto LRUKReplacer::Size() -> size_t {
  Lockker _(latch_);
  _.Lock();
  return node_evict_.size();
}

}  // namespace bustub
