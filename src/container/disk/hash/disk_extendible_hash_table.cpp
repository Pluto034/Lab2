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
  BasicPageGuard Header = this->bpm_->NewPageGuarded(&this->header_page_id_);
  auto header = Header.AsMut<ExtendibleHTableHeaderPage>();
  header->Init(header_max_depth);
  this->index_name_ = name;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  auto hash = this->hash_fn_.GetHash(key);

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

  V tm;
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
  auto hash = this->hash_fn_.GetHash(key);
  WritePageGuard header = this->bpm_->FetchPageWrite(header_page_id_);
  auto *header_page = header.AsMut<ExtendibleHTableHeaderPage>();

  return InsertToNewDirectory(header_page, header_page->HashToDirectoryIndex(hash), hash, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  auto dir_page_id_ = static_cast<page_id_t>(header->GetDirectoryPageId(directory_idx));
  WritePageGuard dir_page;
  if (dir_page_id_ == INVALID_PAGE_ID) {
    // dont have directory
    dir_page = this->bpm_->NewPageGuarded(&dir_page_id_).UpgradeWrite();
    if (dir_page_id_ == INVALID_PAGE_ID) {
      return false;
    }
  } else {
    dir_page = this->bpm_->FetchPageWrite(dir_page_id_);
  }
  auto *bucket_page = dir_page.AsMut<ExtendibleHTableDirectoryPage>();
  return InsertToNewBucket(bucket_page, bucket_page->HashToBucketIndex(hash), key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  auto bucket_page_id_ = (directory->GetBucketPageId(bucket_idx));
  WritePageGuard bucket_page_;
  if (bucket_page_id_ == INVALID_PAGE_ID) {
    // dont have directory
    bucket_page_ = this->bpm_->NewPageGuarded(&bucket_page_id_).UpgradeWrite();
    if (bucket_page_id_ == INVALID_PAGE_ID) {
      return false;
    }
  } else {
    bucket_page_ = this->bpm_->FetchPageWrite(bucket_page_id_);
  }
  //  bucket_page_.As<ExtendibleHTableBucketPage<K,V,KC>>()->IsFull()
  WritePageGuard split_page;
  if (bucket_page_.AsMut<ExtendibleHTableBucketPage<K, V, KC>>()->IsFull()) {
    while (bucket_page_.AsMut<ExtendibleHTableBucketPage<K, V, KC>>()->IsFull())
    {
      if (directory->GetGlobalDepth() == directory->GetLocalDepth(bucket_idx)) {
        directory->IncrGlobalDepth();
      }
      directory->IncrLocalDepth(bucket_idx);


      auto split_page_idx = directory->GetSplitImageIndex(bucket_idx);
      page_id_t split_page_id_ = directory->GetBucketPageId(split_page_idx);
      split_page = this->bpm_->NewPageGuarded(&split_page_id_).UpgradeWrite();
      split_page.AsMut<ExtendibleHTableBucketPage<K, V, KC>>()->Init(bucket_max_size_);


      UpdateDirectoryMapping(directory, split_page_idx, split_page_id_,directory->GetLocalDepth(bucket_idx),(1u << directory->GetLocalDepth(bucket_idx)) - 1);

    }
  }
  bucket_page_.AsMut<ExtendibleHTableBucketPage<K, V, KC>>()->Insert(key, value, cmp_);
  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  directory->Size();
//  for(uint32_t i  = 0 ; i < directory->Size();i++) {
//    directory->SetBucketPageId()
//  }



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
