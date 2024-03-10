//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  global_depth_ = 0;
  std::fill(local_depths_, local_depths_ + HTABLE_DIRECTORY_ARRAY_SIZE, 0);
  std::fill(bucket_page_ids_, bucket_page_ids_ + HTABLE_DIRECTORY_ARRAY_SIZE, INVALID_PAGE_ID);
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  if (bucket_idx >= HTABLE_DIRECTORY_ARRAY_SIZE) return INVALID_PAGE_ID;
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  assert(bucket_idx < HTABLE_DIRECTORY_ARRAY_SIZE);
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  //  auto tmp = bucket_idx ^ (1u << local_depths_[bucket_idx]);
  return bucket_idx ^ (1u << local_depths_[bucket_idx]);
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  assert(global_depth_ < max_depth_);

  for (uint32_t i = (1u << (global_depth_)); i < (1u << (global_depth_ + 1)); i++) {
    local_depths_[i] = local_depths_[i - (1u << (global_depth_))];
    bucket_page_ids_[i] = bucket_page_ids_[i - (1u << (global_depth_))];
  }
  global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  assert(global_depth_ > 0);
  std::fill(local_depths_ + (1u << (global_depth_ - 1)), local_depths_ + ((1u << global_depth_)), 0);
  std::fill(bucket_page_ids_ + (1u << (global_depth_ - 1)), bucket_page_ids_ + ((1u << global_depth_)), -1);
  global_depth_--;
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  return std::all_of(std::begin(local_depths_), std::end(local_depths_),
                     [this](auto item) { return item < global_depth_; });
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1u << global_depth_; }
auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return HTABLE_DIRECTORY_ARRAY_SIZE; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]++; }

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]--; }

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return (1 << this->local_depths_[bucket_idx]) - 1;
}
auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return (1 << global_depth_) - 1; }

}  // namespace bustub
