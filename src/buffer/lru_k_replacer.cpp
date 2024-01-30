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
#include <algorithm>
#include "common/exception.h"
#include "fmt/format.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  bool f = false;
  for (auto p : less_k_) {
    int id = static_cast<int>(p.second);
    if (node_store_[id].is_evictable_ && !f) {
      *frame_id = p.second;
      f = true;
    }
  }
  for (auto p : morethan_k_) {
    int id = static_cast<int>(p.second);
    if (node_store_[id].is_evictable_ && !f) {
      *frame_id = p.second;
      f = true;
    }
  }

  if (f) {
    int id = *frame_id;
    if (node_store_[id].history_.size() >= k_) {
      morethan_k_.erase({mp_[id], id});
    } else {
      less_k_.erase({mp_[id], id});
    }
    mp_.erase(id);
    node_store_[id].history_.clear();
    node_store_.erase(id);
    return true;
  }
  return false;
}
// enum class AccessType { Unknown = 0, Lookup, Scan, Index };
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);

  current_timestamp_++;
  if (node_store_.count(frame_id) != 0U) {
    if (node_store_[frame_id].history_.size() >= k_) {
      morethan_k_.erase({mp_[frame_id], frame_id});
    } else {
      less_k_.erase({mp_[frame_id], frame_id});
    }
  }
  node_store_[frame_id].history_.push_back(current_timestamp_);
  while (node_store_[frame_id].history_.size() > k_) {
    node_store_[frame_id].history_.pop_front();
  }
  mp_[frame_id] = node_store_[frame_id].history_.front();

  if (node_store_[frame_id].history_.size() >= k_) {
    morethan_k_.insert({mp_[frame_id], frame_id});
  } else {
    less_k_.insert({mp_[frame_id], frame_id});
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.count(frame_id) != 0U) {
    node_store_[frame_id].is_evictable_ = set_evictable;
    return;
  }
  throw bustub::Exception(fmt::format("No matching pages@{}.", frame_id));
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.count(frame_id) == 0U) {
    return;
  }
  if (!node_store_[frame_id].is_evictable_) {
    throw bustub::Exception(fmt::format("No matching pages@{}.", frame_id));
  }
  int id = frame_id;
  if (node_store_[id].history_.size() >= k_) {
    morethan_k_.erase({mp_[id], id});
  } else {
    less_k_.erase({mp_[id], id});
  }
  mp_.erase(id);
  node_store_[id].history_.clear();
  node_store_.erase(id);
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  size_t num = 0;
  for (const auto &frame : this->node_store_) {
    if (frame.second.is_evictable_) {
      num++;
    }
  }
  return num;
}
}  // namespace bustub
