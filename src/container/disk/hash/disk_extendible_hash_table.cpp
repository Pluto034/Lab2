//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  //  BasicPageGuard Header = this->bpm_->NewPageGuarded(&this->header_page_id_);
  //  auto header = Header.AsMut<ExtendibleHTableHeaderPage>();
  //  header->Init(header_max_depth);
  //  Header.Drop();
  BasicPageGuard header_guard = bpm_->NewPageGuarded(&header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);
  header_guard.Drop();

  this->index_name_ = name;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto hash = Hash(key);

  ReadPageGuard header = this->bpm_->FetchPageRead(header_page_id_);

  auto dir_idx = header.As<ExtendibleHTableHeaderPage>()->HashToDirectoryIndex(hash);
  page_id_t dir_page_id_ = header.As<ExtendibleHTableHeaderPage>()->GetDirectoryPageId(dir_idx);
  if (dir_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  ReadPageGuard directory = this->bpm_->FetchPageRead(dir_page_id_);
  auto bucket_idx = directory.As<ExtendibleHTableDirectoryPage>()->HashToBucketIndex(hash);
  page_id_t bucket_page_id_ = directory.As<ExtendibleHTableDirectoryPage>()->GetBucketPageId(bucket_idx);
  if (bucket_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  ReadPageGuard bucket_page = this->bpm_->FetchPageRead(bucket_page_id_);
  const auto *tmp = bucket_page.As<ExtendibleHTableBucketPage<K, V, KC>>();

  V tm{};
  if (tmp->Lookup(key, tm, cmp_)) {
    result->clear();
    result->push_back(tm);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  auto hash = Hash(key);

  BasicPageGuard header_guard = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  auto hashMSB = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(hashMSB);
  if (directory_page_id == INVALID_PAGE_ID) {
    InsertToNewDirectory(header_page, hashMSB, hash, key, value);
    directory_page_id = header_page->GetDirectoryPageId(hashMSB);
  }

  BasicPageGuard directory_guard = bpm_->FetchPageBasic(directory_page_id);
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto hashLSB = directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(hashLSB);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return InsertToNewBucket(directory_page, hashLSB, key, value);
  } else {
    BasicPageGuard bucket_guard = bpm_->FetchPageBasic(bucket_page_id);
    auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    if (bucket_page->IsFull()) {
      return SplitInsertBucket(directory_page, hashLSB, key, value);

    } else {
      bucket_page->Insert(key, value, cmp_);
      return true;
    }
  }
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t directory_page_id;
  BasicPageGuard directory_guard = bpm_->NewPageGuarded(&directory_page_id);
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory_page->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, directory_page_id);
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id;
  BasicPageGuard bucket_guard = bpm_->NewPageGuarded(&bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);
  directory->SetBucketPageId(bucket_idx, bucket_page_id);
  bucket_page->Insert(key, value, cmp_);
  return true;
}
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::SplitInsertBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  BasicPageGuard bucket_guard = bpm_->FetchPageBasic(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  if (directory->GetLocalDepth(bucket_idx) == directory_max_depth_) {
    return false;
  }

  auto new_bucket_idx = directory->GetSplitImageIndex(bucket_idx);
  directory->IncrLocalDepth(bucket_idx);

  page_id_t new_bucket_page_id;
  BasicPageGuard new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id);
  auto new_bucket_page = new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  new_bucket_page->Init(bucket_max_size_);

  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
  directory->SetLocalDepth(new_bucket_idx, directory->GetLocalDepth(bucket_idx));

  uint32_t tmp = Hash(key) % (1u << directory->GetLocalDepth(bucket_idx));

  auto mod = 1u << directory->GetLocalDepth(bucket_idx);

  if (directory->GetGlobalDepth() == directory->GetLocalDepth(bucket_idx) - 1) {
    directory->IncrGlobalDepth();
  }

  for (int i = static_cast<int>(bucket_page->Size()) - 1; i >= 0; i--) {
    auto bucket_tmp = bucket_page->EntryAt(i);
    auto hashtmp = Hash(bucket_tmp.first);
    if (hashtmp % mod != tmp) {
      new_bucket_page->Insert(bucket_tmp.first, bucket_tmp.second, cmp_);
      bucket_page->RemoveAt(i);
    }
  }
  //  auto tmp_GlobalDepth = directory->GetGlobalDepth();

  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
  //  for (uint32_t i = 0; i < (1u << (tmp_GlobalDepth + 1)); i++) {
  ////    local_depths_[i] = local_depths_[i - (1u << (global_depth_))];
  //    if(i)
  //    bucket_page_ids_[i] = bucket_page_ids_[i - (1u << (global_depth_))];
  //  }

  return Insert(key, value);

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  return false;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
