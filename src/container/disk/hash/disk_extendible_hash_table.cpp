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
  this->index_name_ = name;
  WritePageGuard head_page = this->bpm_->NewPageGuarded(&this->header_page_id_).UpgradeWrite();
  auto lp_head = head_page.AsMut<ExtendibleHTableHeaderPage>();
  lp_head->Init();
  // throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  if(this->header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  auto hash = this->hash_fn_.GetHash(key);
  ReadPageGuard root_page = this->bpm_->FetchPageRead(this->header_page_id_);
  auto dir_idx = root_page.As<ExtendibleHTableHeaderPage>()->HashToDirectoryIndex(hash);

  page_id_t dir_page_id = root_page.As<ExtendibleHTableHeaderPage>()->GetDirectoryPageId(dir_idx);
  if(dir_page_id == INVALID_PAGE_ID) {
    return false;
  }

  ReadPageGuard dir_page = this->bpm_->FetchPageRead(dir_page_id);
  auto bucket_idx = dir_page.As<ExtendibleHTableDirectoryPage>()->HashToBucketIndex(hash);

  page_id_t bucket_page_id = dir_page.As<ExtendibleHTableDirectoryPage>()->GetBucketPageId(bucket_idx);
  if(bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  ReadPageGuard bucket_page = this->bpm_->FetchPageRead(bucket_page_id);
  const ExtendibleHTableBucketPage<K,V,KC>* lp_page = bucket_page.As<ExtendibleHTableBucketPage<K,V,KC>>();

  V tmp{};
  result->clear();
  bool ret = lp_page->Lookup(key,tmp,this->cmp_);
  if(ret) {
    result->emplace_back(std::move(tmp));
  }
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  auto hash = this->hash_fn_.GetHash(key);
  WritePageGuard root_page = this->bpm_->FetchPageWrite(this->header_page_id_);

  ExtendibleHTableHeaderPage* lp_root = root_page.AsMut<ExtendibleHTableHeaderPage>();

  return this->InsertToNewDirectory(lp_root, lp_root->HashToDirectoryIndex(hash), hash, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  WritePageGuard dir_page;
  // 获取当前槽上的页号
  page_id_t dir_page_id = static_cast<page_id_t>(header->GetDirectoryPageId(directory_idx));
  if(dir_page_id == INVALID_PAGE_ID) {
    dir_page = this->bpm_->NewPageGuarded(&dir_page_id).UpgradeWrite();

    // 如果没有剩余的页, 直接返回失败
    if(dir_page_id == INVALID_PAGE_ID) {
      return false;
    }

    dir_page.AsMut<ExtendibleHTableDirectoryPage>()->Init();
    header->SetDirectoryPageId(directory_idx, dir_page_id);
  }
  else {
    dir_page = this->bpm_->FetchPageWrite(dir_page_id);
  }

  ExtendibleHTableDirectoryPage* page = dir_page.AsMut<ExtendibleHTableDirectoryPage>();
  return this->InsertToNewBucket(page, page->HashToBucketIndex(hash), key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  WritePageGuard bucket_page;
  // 获取当前槽上的页号
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  // 如果当前槽上没有页面
  if(bucket_page_id == INVALID_PAGE_ID) {
    bucket_page = this->bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite();

    // 如果没有剩余的页, 直接返回失败
    if(bucket_page_id == INVALID_PAGE_ID) {
      return false;
    }

    directory->SetBucketPageId(bucket_idx, bucket_page_id);
    bucket_page.AsMut<ExtendibleHTableBucketPage<K,V,KC>>()->Init();
  }
  else {
    bucket_page = this->bpm_->FetchPageWrite(bucket_page_id);
  }

  // 需要分裂
  while(bucket_page.As<ExtendibleHTableBucketPage<K,V,KC>>()->IsFull()) {
    uint32_t split_idx = directory->GetSplitImageIndex(bucket_idx);
    page_id_t split_page_id = directory->GetBucketPageId(split_idx);

    WritePageGuard split_page;
    // 如果还没挂载页面
    if(split_page_id == INVALID_PAGE_ID) {
      if(directory->GetLocalDepth(bucket_idx) < directory->GetGlobalDepth()) {
        
      }
      split_page = this->bpm_->NewPageGuarded(&split_page_id).UpgradeWrite();

      if(split_page_id == INVALID_PAGE_ID) {
        return false;
      }

      split_page.AsMut<ExtendibleHTableBucketPage<K,V,KC>>()->Init();
      directory->SetBucketPageId(split_idx, split_page_id);
    }
  }

  ExtendibleHTableBucketPage<K,V,KC>* lp_bucket = bucket_page.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();

  return lp_bucket->Insert(key, value, this->cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");

  if(this->header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  auto hash = this->hash_fn_.GetHash(key);
  WritePageGuard root_page = this->bpm_->FetchPageWrite(this->header_page_id_);
  auto dir_idx = root_page.As<ExtendibleHTableHeaderPage>()->HashToDirectoryIndex(hash);

  page_id_t dir_page_id = root_page.As<ExtendibleHTableHeaderPage>()->GetDirectoryPageId(dir_idx);
  if(dir_page_id == INVALID_PAGE_ID) {
    return false;
  }

  WritePageGuard dir_page = this->bpm_->FetchPageWrite(dir_page_id);
  auto bucket_idx = dir_page.As<ExtendibleHTableDirectoryPage>()->HashToBucketIndex(hash);

  page_id_t bucket_page_id = dir_page.As<ExtendibleHTableDirectoryPage>()->GetBucketPageId(bucket_idx);
  if(bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  WritePageGuard bucket_page = this->bpm_->FetchPageWrite(bucket_page_id);
  const ExtendibleHTableBucketPage<K,V,KC>* lp_page = bucket_page.As<ExtendibleHTableBucketPage<K,V,KC>>();
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
