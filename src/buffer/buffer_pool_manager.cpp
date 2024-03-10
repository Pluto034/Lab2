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

#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>
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
  //  std::cerr << "NewPage: " << page_id << std::endl;
  //
  //  int64_t tid = syscall((__NR_gettid));
  //  std::cout << "Current thread: " << tid << std::endl;
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

  new_page->pin_count_ = 1;

  *page_id = new_page_id;

  return new_page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  //  std::cerr << "Fetch: " << page_id << std::endl;
  //
  std::unique_lock<std::mutex> lock(latch_);
  //  int64_t tid = syscall((__NR_gettid));
  //  std::cout << "Current thread: " << tid << std::endl;

  auto iter = page_table_.find(page_id);

  if (iter != page_table_.end()) {
    frame_id_t fid = iter->second;

    Page *page = &pages_[fid];

    page->pin_count_++;

    replacer_->RecordAccess(fid);

    replacer_->SetEvictable(fid, false);

    //    if (access_type == AccessType::Scan) {

    //      page->is_dirty_ = true;

    //    }

    return page;
  }

  frame_id_t fid;

  if (not free_list_.empty()) {
    fid = free_list_.front();

    Page *page = pages_ + fid;

    free_list_.pop_front();

    SwapPage(page, page_id);

    page->pin_count_++;

    page_table_[page_id] = fid;

    replacer_->RecordAccess(fid);

    replacer_->SetEvictable(fid, false);

    //    if (access_type == AccessType::Scan) {

    //      page->is_dirty_ = true;

    //    }

    return page;
  }

  if (replacer_->Evict(&fid)) {
    Page *page = pages_ + fid;

    SwapPage(page, page_id);

    page->pin_count_++;

    page_table_[page_id] = fid;

    replacer_->RecordAccess(fid);

    replacer_->SetEvictable(fid, false);

    //    if (access_type == AccessType::Scan) {

    //      page->is_dirty_ = true;

    //    }

    return page;
  }

  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  //  std::cerr << "UnpinPage:" << page_id << " " << is_dirty << std::endl;
  //  int64_t tid = syscall((__NR_gettid));
  //  std::cout << "Current thread: " << tid << std::endl;
  std::unique_lock<std::mutex> lock(latch_);

  if (page_table_.find(page_id) == page_table_.end() || pages_[page_table_[page_id]].GetPinCount() == 0) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];

  Page *page = &pages_[frame_id];

  if (--page->pin_count_ == 0) {
    replacer_->SetEvictable(page_table_[page_id], true);
  }

  page->is_dirty_ |= is_dirty;

  //  pages_[page_id].is_dirty_ = is_dirty;

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  //  std::unique_lock<std::mutex> lock(latch_);

  //  std::cerr << "FlushPage: " << page_id << std::endl;
  //
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
  //  std::cerr << "Delete:" << page_id << std::endl;
  //
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

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  //  std::cerr << "FetchPageBasic" << page_id << "\n";
  //  //  std::unique_ptr<Page> page = std::unique_ptr<Page>(FetchPage(page_id));
  auto page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  //  std::cerr << "FetchPageRead" << page_id << "\n";
  //  //  std::unique_ptr<Page> page = std::unique_ptr<Page>(FetchPage(page_id));
  auto page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  //  std::cerr << "FetchPageWrite" << page_id << "\n";
  //
  //  std::unique_ptr<Page> page = std::unique_ptr<Page>(FetchPage(page_id));
  auto page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  //  std::cerr << "NewPageGuarded" << page_id << "\n";
  //
  //  int64_t tid = syscall((__NR_gettid));
  //  std::cout << "Current thread: " << tid << std::endl;
  auto page = NewPage(page_id);
  return {this, page};
}

auto BufferPoolManager::WritePage(Page *lpPage, bool is_locked) -> void {
  if (lpPage->GetPageId() == INVALID_PAGE_ID) {
    return;
  }
  //  std::cerr << "BufferPoolManager::WritePage"
  //            << " " << lpPage->GetPageId() << "\n";
  if (not is_locked) {
    lpPage->RLatch();
    lpPage->pin_count_++;
  }

  DiskRequest req;

  auto promise = disk_scheduler_->CreatePromise();

  auto future = promise.get_future();

  req.is_write_ = true;

  req.data_ = lpPage->data_;

  req.page_id_ = lpPage->page_id_;

  req.callback_ = std::move(promise);

  disk_scheduler_->Schedule(std::move(req));

  future.get();

  lpPage->is_dirty_ = false;

  if (not is_locked) {
    lpPage->pin_count_--;

    lpPage->RUnlatch();
  }
}

auto BufferPoolManager::ReadPage(Page *lpPage, page_id_t page_to_read, bool is_locked) -> void {
  //  std::cerr << "BufferPoolManager::WritePage"
  //            << " " << lpPage->GetPageId() << "\n";
  if (not is_locked) {
    lpPage->WLatch();
    lpPage->pin_count_++;
  }

  // 如果还有线程在使用页

  BUSTUB_ASSERT(lpPage->pin_count_ != 0, "ReadPage called while page is using by someone.");

  // 如果提供的ID有效

  if (page_to_read != INVALID_PAGE_ID) {
    DiskRequest req;

    auto promise = disk_scheduler_->CreatePromise();

    auto future = promise.get_future();

    req.is_write_ = false;

    req.data_ = lpPage->data_;

    req.page_id_ = page_to_read;

    req.callback_ = std::move(promise);

    disk_scheduler_->Schedule(std::move(req));

    future.get();

  }

  // 如果提供的是个无效ID

  else {
    lpPage->ResetMemory();
  }

  lpPage->is_dirty_ = false;

  lpPage->page_id_ = page_to_read;

  if (not is_locked) {
    lpPage->pin_count_--;

    lpPage->WUnlatch();
  }
}

auto BufferPoolManager::SwapPage(Page *page_to_swap, page_id_t swap_to, bool is_locked) -> void {
  //  std::cerr << "BufferPoolManager::WritePage"
  //            << " " << page_to_swap->GetPageId() << "\n";
  if (not is_locked) {
    page_to_swap->WLatch();
    page_to_swap->pin_count_++;
  }

  if (page_to_swap->IsDirty()) {
    WritePage(page_to_swap, true);
  }

  auto pid = page_to_swap->page_id_;

  auto fid = static_cast<frame_id_t>(page_to_swap - pages_);

  ReadPage(page_to_swap, swap_to, true);

  replacer_->Remove(fid);

  page_table_.erase(pid);

  if (not is_locked) {
    page_to_swap->pin_count_--;

    page_to_swap->WUnlatch();
  }
}

}  // namespace bustub