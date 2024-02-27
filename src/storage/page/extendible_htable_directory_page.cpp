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
  this->max_depth_ = max_depth;
  this->global_depth_ = 0;
  std::fill(std::begin(this->local_depths_),std::end(this->local_depths_),0);
  std::fill(std::begin(this->bucket_page_ids_),std::end(this->bucket_page_ids_),INVALID_PAGE_ID);
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  auto mask = HTABLE_DIRECTORY_ARRAY_SIZE-1;
  return hash&mask;
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return this->bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  this->bucket_page_ids_[bucket_idx] = bucket_page_id;
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  auto depth = this->local_depths_[bucket_idx];
  return (uint32_t(1)<<depth)^bucket_idx;
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return this->global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  this->global_depth_ ++;
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  this->global_depth_ --;
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  return std::all_of(std::begin(this->local_depths_), std::end(this->local_depths_),
                     [this](auto item) { return item < this->global_depth_; });
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
  return uint32_t(1)<<this->global_depth_;
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return this->local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  this->local_depths_[bucket_idx] = local_depth;
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  this->local_depths_[bucket_idx] ++;
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  this->local_depths_[bucket_idx] --;
  // throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

}  // namespace bustub
