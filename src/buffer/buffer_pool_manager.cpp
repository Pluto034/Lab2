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
  //   TODO(students): remove this line after you have implemented the buffer pool manager
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
  std::cerr << "NewPage: " << page_id << std::endl;
  std::unique_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {
    Page *old_page = &pages_[frame_id];  // point frame_id th Page
    if (old_page->IsDirty()) {
      FlushPage(old_page->page_id_);
    }
    page_table_.erase(old_page->page_id_);
  } else {
    return nullptr;
  }
  page_id_t new_page_id = AllocatePage();  // get new page_id
  page_table_[new_page_id] = frame_id;

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  Page *new_page = &pages_[frame_id];
  new_page->page_id_ = new_page_id;
  new_page->is_dirty_ = false;
  new_page->pin_count_ += 1;

  *page_id = new_page_id;
  return new_page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::cerr << "Fetch: " << page_id << std::endl;
  std::unique_lock<std::mutex> lock(latch_);
  if (page_table_.count(page_id) != 0U) {
    frame_id_t frame_id = page_table_[page_id];
    Page *new_page = &pages_[frame_id];
    new_page->pin_count_++;
    new_page->is_dirty_ = true;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return new_page;
  }
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {
    Page *old_page = &pages_[frame_id];  // point frame_id th Page
    if (old_page->IsDirty()) {
      FlushPage(old_page->page_id_);
    }
    page_table_.erase(old_page->page_id_);
  } else {
    return nullptr;
  }
  page_id_t new_page_id = page_id;  // get new page_id

  page_table_[new_page_id] = frame_id;

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, pages_[frame_id].data_, page_id, std::move(promise)});
  future.get();

  Page *new_page = &pages_[frame_id];
  new_page->page_id_ = new_page_id;
  new_page->is_dirty_ = false;
  new_page->pin_count_ = 1;
  //  new_page->ResetMemory();

  //  *page_id = new_page_id;
  return new_page;

  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::cerr << "UnpinPage:" << page_id << " " << is_dirty << std::endl;
  std::unique_lock<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];

  page->is_dirty_ |= is_dirty;
  if (pages_[page_table_[page_id]].GetPinCount() == 0) {
    return false;
  }
  if (--page->pin_count_ == 0) {
    replacer_->SetEvictable(page_table_[page_id], true);
  }
  //  page->is_dirty_ = is_dirty;
  //  pages_[page_id].is_dirty_ = is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  //  std::unique_lock<std::mutex>lock(latch_
  ;
  std::cerr << "FlushPage: " << page_id << std::endl;
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  page->is_dirty_ = false;
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, page->data_, page_id, std::move(promise)});
  future.get();
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::unique_lock<std::mutex> lock(latch_);
  for (auto p : page_table_) {
    FlushPage(p.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::cerr << "Delete:" << page_id << std::endl;
  std::unique_lock<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0U) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->pin_count_ != 0) {
    return false;
  }
  if (page->is_dirty_) {
    FlushPage(page_id);
  }

  page_table_.erase(page->page_id_);
  free_list_.push_back(frame_id);
  replacer_->Remove(frame_id);

  page->ResetMemory();
  //  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
