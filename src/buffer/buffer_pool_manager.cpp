//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
//  throw NotImplementedException(
//      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
//      "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> _(latch_);
  frame_id_t fid = 0;
  // 如果有空闲页, 优先分配空闲页
  if (not free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();

    auto &page = pages_[fid];

    // 分配页id
    page_id_t pid = AllocatePage();
    this->page_table_[pid] = fid;
    page.page_id_ = pid;
    replacer_->RecordAccess(fid);

    *page_id = pid;

    page.ResetMemory();

    // 引用计数加1
    page.pin_count_++;
    replacer_->SetEvictable(fid, false);
    page.is_dirty_ = true;

    return &page;
  }

  // 如果有可以置换的页
  if(replacer_->Evict(&fid)) {
    auto& page = pages_[fid];

    // 如果页在缓冲池中已经被修改过, 将修改回写
    DumpPage(page);
    page.ResetMemory();

    replacer_->RecordAccess(fid);

    // 引用计数加1
    page.pin_count_++;
    replacer_->SetEvictable(fid, false);
    page.is_dirty_ = true;

    page_id_t pid = AllocatePage();
    *page_id = pid;
    page.page_id_ = pid;

    page_table_[pid] = fid;

    return &page;
  }

  // 分配失败
  *page_id = INVALID_PAGE_ID;
  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock<std::mutex> _(latch_);
  if(page_id == INVALID_PAGE_ID or page_table_.count(page_id) == 0U) {
    return nullptr;
  }

  frame_id_t fid = page_table_[page_id];

  // 需要的页在当前缓冲池中正确的位置上
  if(pages_[fid].page_id_ == page_id) {
    auto &page = pages_[fid];

    // 引用计数加1
    page.pin_count_++;
    replacer_->SetEvictable(fid, false);
    page.is_dirty_ = true;

    return &page;
  }

  // 否则说明页不在缓冲池, 在磁盘上
  // 如果缓冲池有空闲, 先使用空闲的缓冲池
  if(not free_list_.empty()) {
    fid = free_list_.front(); free_list_.pop_front();

    auto& page = pages_[fid];

    LoadPage(page, page_id);

    replacer_->RecordAccess(fid);

    // 引用计数加1
    page.pin_count_++;
    replacer_->SetEvictable(fid, false);
    page.is_dirty_ = true;

    return &page;
  }

  // 如果有可以换出的页面
  if(replacer_->Evict(&fid)) {
    auto& page = pages_[fid];

    // 将页面写回磁盘
    DumpPage(page);

    LoadPage(page, page_id);

    replacer_->Remove(fid);
    replacer_->RecordAccess(fid);

    // 引用计数加1
    page.pin_count_++;
    replacer_->SetEvictable(fid, false);
    page.is_dirty_ = true;

    return &page;
  }

  // 如果没有空闲页面并且也没有可以换出的页面, 访问失败
  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock<std::mutex> _(latch_);
  // 理论上不可能的情况
  if(page_table_.count(page_id) == 0U) {
    return false;
  }
  frame_id_t fid = page_table_.at(page_id);
  auto& page = pages_[fid];
  if(page.pin_count_ <= 0) {
    return false;
  }

  if(is_dirty) {
    page.is_dirty_ = true;
  }

  page.pin_count_--;
  if(page.pin_count_ == 0) {
    replacer_->SetEvictable(fid, true);
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> _(latch_);
  if(page_table_.count(page_id) == 0U) {
    return false;
  }
  frame_id_t frame_id = page_table_.at(page_id);
  auto& page = pages_[frame_id];
  return DumpPage(page);
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> _(latch_);
  for(auto[_,fid] :page_table_) {
    auto& page = pages_[fid];
    DumpPage(page);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> _(latch_);
  if(page_table_.count(page_id) == 0U) {
    return true;
  }
  frame_id_t fid = page_table_.at(page_id);
  auto& page = pages_[fid];
  if(page.pin_count_ == 0) {
    if(page.is_dirty_) {
      DumpPage(page);
    }
    replacer_->Remove(fid);
    page_table_.erase(page_id);
    return true;
  }
  return false;
}

auto BufferPoolManager::DumpPage(Page &page) -> bool {
  page.WLatch();

  if(page.page_id_ == INVALID_PAGE_ID or not page.is_dirty_) {
    page.rwlatch_.WUnlock();
    return false;
  }

  DiskRequest req;

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();

  req.is_write_ = true;
  req.callback_ = std::move(promise);
  req.data_ = page.data_;
  req.page_id_ = page.page_id_;

  disk_scheduler_->Schedule(std::move(req));

  bool res = future.get();

  // 如果写成功, 那么页面就不是脏的
  page.is_dirty_ = !res;

  page.WUnlatch();

  return res;
}

auto BufferPoolManager::LoadPage(Page &page, page_id_t pid) -> bool {
  page.WLatch();
  // 理论上磁盘数据不可能比内存新
  // 但是如果就是需要用旧数据覆盖, 就可能了
  // if(page.page_id_ == pid) {
  //   return false;
  // }

  //BUSTUB_ASSERT(page.pin_count_ == 0, "FOR LOAD OPERATION, YOU SHOULD ENSURE NO REF COUNT!");

  DiskRequest req;

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();

  req.is_write_ = false;
  req.callback_ = std::move(promise);
  req.data_ = page.data_;
  req.page_id_ = pid;

//  page.RLatch();
  disk_scheduler_->Schedule(std::move(req));

  bool res = future.get();
//  page.RUnlatch();

  if (res) {
    page.page_id_ = pid;
    page.is_dirty_ = false;
    page.pin_count_ = 0;
  }

  page.WUnlatch();

  return res;
}


auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
