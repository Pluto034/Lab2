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
  // std::cerr << "k:" << replacer_k << std::endl;
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  std::scoped_lock<std::mutex> _(latch_);
  this->replacer_.reset();
  delete[] pages_;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
 * are currently in use and not evictable (in another word, pinned).
 *
 * You should pick the replacement frame from either the free list or the replacer (always find from the free list
 * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
 * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
 *
 * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
 * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
 * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
 *
 * @param[out] page_id id of created page
 * @return nullptr if no new pages could be created, otherwise pointer to new page
 */
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  // 加管理锁
  std::scoped_lock<std::mutex> _(latch_);

  Page *res = nullptr;
  frame_id_t fid = INVALID_PAGE_ID;
  if (not free_list_.empty()) {
    *page_id = AllocatePage();

    fid = free_list_.front();
    free_list_.pop_front();
    res = &pages_[fid];
  } else if (replacer_->Evict(&fid)) {
    *page_id = AllocatePage();

    res = &pages_[fid];
    SwapPage(res, INVALID_PAGE_ID);
  } else {
    fid = INVALID_PAGE_ID;
    res = nullptr;
  }

  if (res == nullptr) {
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }
  res->page_id_ = *page_id;

  page_table_[*page_id] = fid;

  res->pin_count_++;
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);

  WritePage(res);
  return res;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
 * but all frames are currently in use and not evictable (in another word, pinned).
 *
 * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
 * the replacer (always find from the free list first), read the page from disk by scheduling a read DiskRequest with
 * disk_scheduler_->Schedule(), and replace the old page in the frame. Similar to NewPage(), if the old page is dirty,
 * you need to write it back to disk and update the metadata of the new page
 *
 * In addition, remember to disable eviction and record the access history of the frame like you did for NewPage().
 *
 * @param page_id id of page to be fetched
 * @param access_type type of access to the page, only needed for leaderboard tests.
 * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
 */
auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock<std::mutex> _(latch_);
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    frame_id_t fid = iter->second;
    Page *page = &pages_[fid];

    page->pin_count_++;
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);

    if (access_type == AccessType::Scan) {
      page->is_dirty_ = true;
    }

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

    if (access_type == AccessType::Scan) {
      page->is_dirty_ = true;
    }

    return page;
  }

  if (replacer_->Evict(&fid)) {
    Page *page = pages_ + fid;
    SwapPage(page, page_id);

    page->pin_count_++;
    page_table_[page_id] = fid;
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);

    if (access_type == AccessType::Scan) {
      page->is_dirty_ = true;
    }

    return page;
  }

  return nullptr;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
 * 0, return false.
 *
 * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
 * Also, set the dirty flag on the page to indicate if the page was modified.
 *
 * @param page_id id of page to be unpinned
 * @param is_dirty true if the page should be marked as dirty, false otherwise
 * @param access_type type of access to the page, only needed for leaderboard tests.
 * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
 */
auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock<std::mutex> _(latch_);

  auto iter = page_table_.find(page_id);

  if (iter == page_table_.end()) {
    return false;
  }

  Page *page = &pages_[iter->second];

  if (page->pin_count_ <= 0) {
    return false;
  }

  if (is_dirty) {
    page->is_dirty_ = true;
  }

  page->pin_count_--;
  replacer_->SetEvictable(iter->second, true);
  return true;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Flush the target page to disk.
 *
 * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
 * Unset the dirty flag of the page after flushing.
 *
 * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
 * @return false if the page could not be found in the page table, true otherwise
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> _(latch_);

  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }

  Page *page = &pages_[iter->second];
  if (page->IsDirty()) {
    WritePage(page);
  }
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> _(latch_);

  for (const auto [_, fid] : page_table_) {
    auto page = &pages_[fid];
    if (page->IsDirty()) {
      WritePage(page);
    }
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
 * page is pinned and cannot be deleted, return false immediately.
 *
 * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
 * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
 * imitate freeing the page on the disk.
 *
 * @param page_id id of page to be deleted
 * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> _(latch_);

  auto iter = page_table_.find(page_id);

  if (iter == page_table_.end()) {
    return true;
  }

  Page *page = &pages_[iter->second];
  if (page->pin_count_ > 0) {
    return false;
  }

  frame_id_t fid = iter->second;

  page_table_.erase(page_id);
  replacer_->Remove(fid);

  SwapPage(page, INVALID_PAGE_ID);
  free_list_.push_back(fid);

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto page = NewPage(page_id);
  return {this, page};
}

/**
 * 只将指定页写入磁盘, 不管页面是否是脏的
 * 如果当前页面的id无效, 那么不做任何事
 * 使用之前请上管理锁
 * @param lpPage 需要写入磁盘的页面的指针
 * @param is_locked 使用之前页面是否已经上写锁
 */
auto BufferPoolManager::WritePage(Page *lpPage, bool is_locked) -> void {
  if (lpPage->GetPageId() == INVALID_PAGE_ID) {
    return;
  }

  if (not is_locked) {
    // lpPage->RLatch();
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
    // lpPage->RUnlatch();
  }
}

/**
 * 只将指定页面编号的页面读入内存
 * 如果page_to_read是INVALID_PAGE_ID, 那么将页初始化
 * 如果页面脏, 不会自动写入磁盘, 如果需要自动写入, 请使用SwapPage
 * 使用之前请上管理锁
 * @param lpPage 需要读入的页面的指针
 * @param page_to_read 指定页面的编号
 * @param is_locked 使用之前页面是否已经上写锁
 */
auto BufferPoolManager::ReadPage(Page *lpPage, page_id_t page_to_read, bool is_locked) -> void {
  if (not is_locked) {
    // lpPage->WLatch();
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
    // lpPage->WUnlatch();
  }
}

/**
 * 将指定页面交换进内存
 * 如果当前页面脏, 会自动写入磁盘
 * 相当于一次Write(如果页面脏的话), 一次Read
 * 使用之前请上管理锁
 * @param page_to_swap 需要交换的页面的指针
 * @param swap_to 交换成的页面id
 * @param is_locked 使用之前页面是否已经上写锁
 */
auto BufferPoolManager::SwapPage(Page *page_to_swap, page_id_t swap_to, bool is_locked) -> void {
  if (not is_locked) {
    // page_to_swap->WLatch();
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
    // page_to_swap->WUnlatch();
  }
}

}  // namespace bustub
