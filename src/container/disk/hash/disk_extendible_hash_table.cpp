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
  if (this->header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  auto hash = Hash(key);
  ReadPageGuard root_page = this->bpm_->FetchPageRead(this->header_page_id_);
  auto dir_idx = root_page.As<ExtendibleHTableHeaderPage>()->HashToDirectoryIndex(hash);

  page_id_t dir_page_id = root_page.As<ExtendibleHTableHeaderPage>()->GetDirectoryPageId(dir_idx);
  if (dir_page_id == INVALID_PAGE_ID) {
    return false;
  }

  ReadPageGuard dir_page = this->bpm_->FetchPageRead(dir_page_id);
  auto bucket_idx = dir_page.As<ExtendibleHTableDirectoryPage>()->HashToBucketIndex(hash);

  page_id_t bucket_page_id = dir_page.As<ExtendibleHTableDirectoryPage>()->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  ReadPageGuard bucket_page = this->bpm_->FetchPageRead(bucket_page_id);
  const auto *lp_page = bucket_page.As<ExtendibleHTableBucketPage<K, V, KC>>();

  V tmp{};
  result->clear();
  bool ret = lp_page->Lookup(key, tmp, this->cmp_);
  if (ret) {
    result->emplace_back(std::move(tmp));
  }
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  std::cerr << "size: " << this->bpm_->GetPoolSize() << std::endl;
  auto hash = Hash(key);
  if (hash == 511) {
    this->PrintHT();
  }
  WritePageGuard root_page =
      this->GetWriteablePage<ExtendibleHTableHeaderPage>(this->header_page_id_, this->header_max_depth_);
  ExtendibleHTableHeaderPage *lp_root_page = root_page.AsMut<ExtendibleHTableHeaderPage>();

  auto directory_idx = lp_root_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = lp_root_page->GetDirectoryPageId(directory_idx);
  WritePageGuard directory_page =
      this->GetWriteablePage<ExtendibleHTableDirectoryPage>(directory_page_id, this->directory_max_depth_);
  lp_root_page->SetDirectoryPageId(directory_idx, directory_page_id);
  ExtendibleHTableDirectoryPage *lp_directory_page = directory_page.AsMut<ExtendibleHTableDirectoryPage>();

  // TODO 改掉这个缺德事
  root_page.Drop();

  auto bucket_idx = lp_directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = lp_directory_page->GetBucketPageId(bucket_idx);
  WritePageGuard bucket_page =
      this->GetWriteablePage<ExtendibleHTableBucketPage<K, V, KC>>(bucket_page_id, this->bucket_max_size_);
  lp_directory_page->SetBucketPageId(bucket_idx, bucket_page_id);
  ExtendibleHTableBucketPage<K, V, KC> *lp_bucket_page = bucket_page.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  if (lp_bucket_page->contains(key, this->cmp_)) {
    return false;
  }

  return InsertToBucket(lp_directory_page, bucket_idx, lp_bucket_page, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  std::cerr << "Remove: " << key << std::endl;
  auto hash = Hash(key);
  WritePageGuard root_page =
      this->GetWriteablePage<ExtendibleHTableHeaderPage>(this->header_page_id_, this->header_max_depth_);
  ExtendibleHTableHeaderPage *lp_root_page = root_page.AsMut<ExtendibleHTableHeaderPage>();

  auto directory_idx = lp_root_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = lp_root_page->GetDirectoryPageId(directory_idx);
  WritePageGuard directory_page =
      this->GetWriteablePage<ExtendibleHTableDirectoryPage>(directory_page_id, this->directory_max_depth_);
  lp_root_page->SetDirectoryPageId(directory_idx, directory_page_id);

  // TODO 改掉这个缺德事
  root_page.Drop();

  ExtendibleHTableDirectoryPage *lp_directory_page = directory_page.AsMut<ExtendibleHTableDirectoryPage>();

  auto bucket_idx = lp_directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = lp_directory_page->GetBucketPageId(bucket_idx);
  WritePageGuard bucket_page =
      this->GetWriteablePage<ExtendibleHTableBucketPage<K, V, KC>>(bucket_page_id, this->bucket_max_size_);
  lp_directory_page->SetBucketPageId(bucket_idx, bucket_page_id);
  ExtendibleHTableBucketPage<K, V, KC> *lp_bucket_page = bucket_page.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  if (not lp_bucket_page->contains(key, this->cmp_)) {
    return false;
  }

  if (not lp_bucket_page->Remove(key, this->cmp_)) {
    return false;
  }

  return Merge(lp_directory_page, lp_bucket_page, bucket_page_id, bucket_idx);
}

template <typename K, typename V, typename KC>
template <typename PageType>
auto DiskExtendibleHashTable<K, V, KC>::GetWriteablePage(page_id_t &lp_page, uint32_t parm) -> WritePageGuard {
  static_assert(sizeof(PageType) <= BUSTUB_PAGE_SIZE);
  if (lp_page == INVALID_PAGE_ID) {
    BasicPageGuard ret = this->bpm_->NewPageGuarded(&lp_page);
    ret.AsMut<PageType>()->Init(parm);
    return ret.UpgradeWrite();
  }
  return this->bpm_->FetchPageWrite(lp_page);
}

template <typename K, typename V, typename KC>
template <typename PageType>
auto DiskExtendibleHashTable<K, V, KC>::GetReadablePage(page_id_t &lp_page, uint32_t parm) -> ReadPageGuard {
  static_assert(sizeof(PageType) <= BUSTUB_PAGE_SIZE);
  if (lp_page == INVALID_PAGE_ID) {
    BasicPageGuard ret = this->bpm_->NewPageGuarded(&lp_page);
    ret.AsMut<PageType>()->Init(parm);
    return ret.UpgradeRead();
  }
  return this->bpm_->FetchPageRead(lp_page);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToBucket(ExtendibleHTableDirectoryPage *lp_directory_page,
                                                       uint32_t bucket_idx,
                                                       ExtendibleHTableBucketPage<K, V, KC> *lp_bucket_page,
                                                       const K &key, const V &value) -> bool {
  while (lp_bucket_page->IsFull()) {
    while (lp_directory_page->GetLocalDepth(bucket_idx) >= lp_directory_page->GetGlobalDepth() and
           lp_directory_page->GetGlobalDepth() < lp_directory_page->GetMaxDepth()) {
      lp_directory_page->IncrGlobalDepth();
    }
    if (lp_directory_page->GetLocalDepth(bucket_idx) >= lp_directory_page->GetGlobalDepth()) {
      return false;
    }
    auto size = lp_bucket_page->Size();
    auto mask = lp_directory_page->GetLocalDepthMask(bucket_idx);

    WritePageGuard split_bucket_page;
    page_id_t split_bucket_page_id = INVALID_PAGE_ID;
    ExtendibleHTableBucketPage<K, V, KC> *lp_split_bucket_page = nullptr;
    auto split_bucket_idx = lp_directory_page->GetSplitImageIndex(bucket_idx);

    lp_directory_page->IncrLocalDepth(bucket_idx);
    lp_directory_page->SetBucketPageId(split_bucket_idx, INVALID_PAGE_ID);
    lp_directory_page->SetLocalDepth(split_bucket_idx, lp_directory_page->GetLocalDepth(bucket_idx));

    for (decltype(size) i = ((mask <<= 1, mask |= 1), 0); i < size; i++) {
      auto &[k, v] = lp_bucket_page->EntryAt(i);
      auto i_hash = Hash(k);
      if ((i_hash & mask) == bucket_idx) continue;

      if (lp_split_bucket_page == nullptr) {
        split_bucket_page =
            this->GetWriteablePage<ExtendibleHTableBucketPage<K, V, KC>>(split_bucket_page_id, this->bucket_max_size_);
        lp_split_bucket_page = split_bucket_page.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
        lp_directory_page->SetBucketPageId(split_bucket_idx, split_bucket_page_id);
      }

      BUSTUB_ASSERT(lp_split_bucket_page != nullptr, "lp_split_bucket_page is nullptr!");

      auto res = this->InsertToBucket(lp_directory_page, split_bucket_idx, lp_split_bucket_page, k, v);
      BUSTUB_ENSURE(res == true, "Fail to Insert.");

      lp_bucket_page->RemoveAt(i);
    }
  }

  auto hash = Hash(key);
  if (lp_directory_page->HashToBucketIndex(hash) != bucket_idx) {
    bucket_idx = lp_directory_page->HashToBucketIndex(hash);

    page_id_t bucket_page_id = lp_directory_page->GetBucketPageId(bucket_idx);
    WritePageGuard bucket_page =
        this->GetWriteablePage<ExtendibleHTableBucketPage<K, V, KC>>(bucket_page_id, this->bucket_max_size_);

    lp_bucket_page = bucket_page.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  }
  return lp_bucket_page->Insert(key, value, this->cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Merge(ExtendibleHTableDirectoryPage *lp_directory_page,
                                              const ExtendibleHTableBucketPage<K, V, KC> *lp_bucket_page,
                                              page_id_t bucket_page_id, uint32_t bucket_idx) -> bool {
  auto size = lp_directory_page->Size();
  auto bucket_depth = lp_directory_page->GetLocalDepth(bucket_idx);
  while (bucket_depth > 0) {
    uint32_t hi_bit = 1ULL << (bucket_depth - 1);
    uint32_t split_bucket_idx = bucket_idx ^ hi_bit;

    page_id_t split_bucket_page_id = lp_directory_page->GetBucketPageId(split_bucket_idx);
    if (split_bucket_page_id == bucket_page_id) {
      bucket_depth--;
      continue;
    }

    if (lp_directory_page->GetLocalDepth(split_bucket_idx) != bucket_depth) {
      break;
    }

    {
      ReadPageGuard split_bucket_page = this->bpm_->FetchPageRead(split_bucket_page_id);
      const ExtendibleHTableBucketPage<K, V, KC> *lp_split_bucket_page =
          split_bucket_page.As<ExtendibleHTableBucketPage<K, V, KC>>();

      if (lp_split_bucket_page->Size() != 0) {
        if (lp_bucket_page->Size() != 0) {
          break;
        }
        std::swap(lp_bucket_page, lp_split_bucket_page);
        std::swap(bucket_page_id, split_bucket_page_id);
      }
    }
    bucket_depth--;

    for (decltype(size) i = 0; i < size; i++) {
      if (lp_directory_page->GetBucketPageId(i) != split_bucket_page_id) {
        continue;
      }
      lp_directory_page->SetBucketPageId(i, bucket_page_id);
      lp_directory_page->SetLocalDepth(i, bucket_depth);
    }
    this->bpm_->DeletePage(split_bucket_page_id);
    bucket_idx &= hi_bit - 1;
  }

  for (decltype(size) i = 0; i < size; i++) {
    if (lp_directory_page->GetBucketPageId(i) != bucket_page_id) {
      continue;
    }
    lp_directory_page->SetLocalDepth(i, bucket_depth);
  }

  while (lp_directory_page->CanShrink()) {
    lp_directory_page->DecrGlobalDepth();
  }

  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
