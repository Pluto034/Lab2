//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include "common/exception.h"

namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  this->max_depth_ = max_depth;
  std::fill(std::begin(this->directory_page_ids_), std::end(this->directory_page_ids_), INVALID_PAGE_ID);
  // throw NotImplementedException("ExtendibleHTableHeaderPage is not implemented");
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  if (this->max_depth_ == 0) return 0;
  return hash >> (32 - this->max_depth_);
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t {
  uint32_t max_idx = 1 << this->max_depth_;
  BUSTUB_ENSURE(directory_idx < max_idx, "directory_idx must be within a valid subscript");
  return this->directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  uint32_t max_idx = 1 << this->max_depth_;
  BUSTUB_ENSURE(directory_idx < max_idx, "directory_idx must be within a valid subscript");
  this->directory_page_ids_[directory_idx] = directory_page_id;
  // throw NotImplementedException("ExtendibleHTableHeaderPage is not implemented");
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t {
  return std::min(HTABLE_HEADER_ARRAY_SIZE, (uint64_t(1)) << this->max_depth_);
}

}  // namespace bustub
