//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.cpp
//
// Identification: src/storage/page/extendible_htable_bucket_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <optional>
#include <utility>

#include "common/exception.h"
#include "storage/page/extendible_htable_bucket_page.h"

namespace bustub {

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Init(uint32_t max_size) {
  this->size_ = 0;
  this->max_size_ = max_size;
  // throw NotImplementedException("ExtendibleHTableBucketPage not implemented");
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K &key, V &value, const KC &cmp) const -> bool {
  for(uint32_t i = 0;i<this->size_;i++) {
    const auto& item = this->array_[i];
    if(cmp(item.first, key) == 0) {
      value = item.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K &key, const V &value, const KC &cmp) -> bool {
  for(uint32_t i = 0;i<this->size_;i++) {
    auto& item = this->array_[i];
    if(cmp(item.first, key) == 0) {
      item.second = value;
      return true;
    }
  }
  if(this->size_>= this->max_size_) {
    return false;
  }
  this->array_[this->size_-1] = std::make_pair(key,value);
  return true;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K &key, const KC &cmp) -> bool {
  std::size_t idx = -1;
  for(std::size_t i = 0;i<this->size_;i++) {
    auto& item = this->array_[i];
    if(cmp(item.first, key) == 0) {
      idx = i;
      break;
    }
  }
  if(idx == (std::size_t)-1) {
    return false;
  }
  for(std::size_t i = idx+1;i<this->size_;i++) {
    this->array_[i-1] = std::move(this->array_[i]);
  }
  this->size_--;
  return true;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(uint32_t bucket_idx) {
  for(std::size_t i = bucket_idx+1;i<this->size_;i++) {
    this->array_[i-1] = std::move(this->array_[i]);
  }
  this->size_--;
  // throw NotImplementedException("ExtendibleHTableBucketPage not implemented");
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(uint32_t bucket_idx) const -> K {
  const auto& item = this->array_[bucket_idx];
  return item.first;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(uint32_t bucket_idx) const -> V {
  const auto& item = this->array_[bucket_idx];
  return item.second;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(uint32_t bucket_idx) const -> const std::pair<K, V> & {
  return this->array_[bucket_idx];
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
  return this->size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
  return this->size_ == this->max_size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
  return false;
}

template class ExtendibleHTableBucketPage<int, int, IntComparator>;
template class ExtendibleHTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
