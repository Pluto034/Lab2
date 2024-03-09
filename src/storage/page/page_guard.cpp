#include "storage/page/page_guard.h"
#include <sys/syscall.h>
#include <unistd.h>
#include <iostream>
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept

    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
//  std::cerr << "BasicPageGuard"
//            << "\n";
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
//  std::cerr << "Drop"
//            << "\n";
//  //  std::cerr << page_ << "\n";
//
  if (page_ == nullptr || bpm_ == nullptr) {
    return;
  }

//  std::cerr << page_->GetPageId() << "\n";
//
  if (is_dirty_) {
    bpm_->FlushPage(page_->GetPageId());
  }
  bpm_->UnpinPage(page_->GetPageId(), page_->IsDirty());
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
//  std::cerr << "BasicPageGuard="
//            << "\n";
  if (this == &that) {
    return *this;
  }
  Drop();
  page_ = that.page_;
  bpm_ = that.bpm_;
  is_dirty_ = that.is_dirty_;
  that.page_ = nullptr;
  that.bpm_ = nullptr;
  that.is_dirty_ = false;
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
//  std::cerr << "BasicPageGuard"
//            << "\n";
  Drop();
}

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
//  std::cerr << "UpgradeRead"
//            << "\n";
  int64_t tid = syscall((__NR_gettid));
  std::cout << "Current thread: " << tid << std::endl;
  if (page_ == nullptr) {
    return {nullptr, nullptr};
  }
  auto new_bpm = bpm_;
  auto new_page = page_;
  new_page->RLatch();
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;

  return {new_bpm, new_page};
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) {
//  std::cerr << "ReadPageGuard"
//            << "\n";
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
//  std::cerr << "ReadPageGuard="
//            << "\n";
  if (this == &that) {
    return *this;
  }

  ReadPageGuard::Drop();
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
  that.guard_.is_dirty_ = false;
  return *this;
}

void ReadPageGuard::Drop() {
//  std::cerr << "ReadPageGuardDrop"
//            << "\n";
  if (guard_.page_ == nullptr) {
    return;
  }
  guard_.page_->RUnlatch();
  guard_.Drop();
  //    guard_.page_->RUnlatch();
}

ReadPageGuard::~ReadPageGuard() {
//  std::cerr << "~ReadPageGuard"
//            << "\n";
  //  if(guard_.page_ == nullptr) return;
  ReadPageGuard::Drop();
}  // NOLINT

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
//  std::cerr << "UpgradeWrite"
//            << "\n";
  int64_t tid = syscall((__NR_gettid));
  std::cout << "Current thread: " << tid << std::endl;
  if (page_ == nullptr) {
    return {nullptr, nullptr};
  }
  auto new_bpm = bpm_;
  auto new_page = page_;
  new_page->WLatch();
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;

  return {new_bpm, new_page};

};  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)) {
//  std::cerr << "WritePageGuard"
//            << "\n";
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
//  std::cerr << "WritePageGuard="
//            << "\n";
  if (this == &that) {
    return *this;
  }
  WritePageGuard::Drop();
  guard_.bpm_ = that.guard_.bpm_;
  guard_.page_ = that.guard_.page_;
  guard_.is_dirty_ = that.guard_.is_dirty_;
  that.guard_.bpm_ = nullptr;
  that.guard_.page_ = nullptr;
  that.guard_.is_dirty_ = false;

  return *this;
}

void WritePageGuard::Drop() {
//  std::cerr << "WritePageGuardDrop"
//            << "\n";
  if (guard_.page_ == nullptr) {
    return;
  }
  guard_.page_->WUnlatch();
  guard_.Drop();

  //  guard_.page_->WUnlatch();
}
WritePageGuard::~WritePageGuard() {
//  std::cerr << "~WritePageGuard"
//            << "\n";
  //  if(guard_.page_ == nullptr) return;
  WritePageGuard::Drop();
}  // NOLINT
}  // namespace bustub