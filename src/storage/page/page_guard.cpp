#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

auto Out() -> std::ostream & { return (std::cerr); }

namespace bustub {

/**
 * 构造函数
 * @param bpm 页面管理器
 * @param page 页面指针
 */
BasicPageGuard::BasicPageGuard(BufferPoolManager *bpm, Page *page) : bpm_(bpm), page_(page) { this->is_dirty_ = false; }

/**
 * 移动构造函数
 * 使用完后that清空
 * @param that
 */
BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  if (that.page_ == nullptr) {
    // std::cerr << "Warn: the page move to BasicPageGuard Object @" << this << " is nullptr." << std::endl;
  }

  this->Copy(that);
  that.Reset();
}

auto BasicPageGuard::Copy(const BasicPageGuard &other) -> void {
  this->bpm_ = other.bpm_;
  this->page_ = other.page_;
  this->is_dirty_ = other.is_dirty_;
}

/**
 * 将页面舍弃
 * 并将当前对象无效化
 */
void BasicPageGuard::Drop() {
  if (this->page_ == nullptr) {
    // std::cerr << "Warn: Drop function was called on a BasicPageGuard Object @" << this << " which page is nullptr."
    //           << std::endl;
  } else {
    // 将修改回写
    this->bpm_->UnpinPage(this->PageId(), this->is_dirty_);
  }

  // 将当前的对象无效化
  this->Reset();
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // 自赋值
  if (&that == this) {
    // std::cerr << "Warn: assign function was called on a BasicPageGuard Object @" << this
    //           << " which the other object is self." << std::endl;
    return *this;
  }

  // 如果当前对象有页
  if (this->page_ != nullptr) {
    this->Drop();
  }

  if (that.page_ == nullptr) {
    // std::cerr << "Warn: the page move to BasicPageGuard Object @" << this << " is nullptr." << std::endl;
  }

  this->Copy(that);
  that.Reset();

  return *this;
}

BasicPageGuard::~BasicPageGuard() { this->Drop(); }
auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  if (this->page_ != nullptr) {
    if (this->is_dirty_) {
      this->bpm_->FlushPage(this->PageId());
      this->is_dirty_ = false;
    }
    this->page_->RLatch();
  }

  return ReadPageGuard{std::move(*this)};
}
auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  if (this->page_ != nullptr) {
    this->page_->WLatch();
  }

  return WritePageGuard{std::move(*this)};
}
auto BasicPageGuard::Reset() -> void {
  this->bpm_ = nullptr;
  this->page_ = nullptr;
  this->is_dirty_ = false;
}
auto BasicPageGuard::PageId() -> page_id_t {
  if (this->page_ == nullptr) {
    std::cerr << "Fail to get page id: BasicPageGuard @" << this << " is invalid." << std::endl;
    throw Exception("Try to get id on a invalid page.");
  }

  return this->page_->GetPageId();
}
auto BasicPageGuard::GetData() -> const char * {
  if (this->page_ == nullptr) {
    std::cerr << "Fail to get page data: BasicPageGuard @" << this << " is invalid." << std::endl;
    throw Exception("Try to get data on a invalid page.");
  }

  return this->page_->GetData();
};  // NOLINT

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) : guard_(bpm, page) {
  if (page == nullptr) {
    // std::cerr << "Warn: ReadPageGuard @" << this << " has no page." << std::endl;
  }
}

ReadPageGuard::ReadPageGuard(BasicPageGuard &&basic) : guard_(std::move(basic)) {}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (&that == this) {
    // std::cerr << "Warn: assign function was called on a ReadPageGuard Object @" << this
    //           << " which the other object is self." << std::endl;
    return *this;
  }
  if (this->guard_.page_ == nullptr) {
    // std::cerr << "Warn: ReadPageGuard @" << this << " has no page." << std::endl;
  } else {
    this->Drop();
  }

  this->guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (this->guard_.page_ != nullptr) {
    this->guard_.page_->RUnlatch();
  }
  this->guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { this->Drop(); }
auto ReadPageGuard::PageId() -> page_id_t { return this->guard_.PageId(); }
auto ReadPageGuard::GetData() -> const char * { return this->guard_.GetData(); }
// NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) : guard_(bpm, page) {
  if (page == nullptr) {
    // std::cerr << "Warn: ReadPageGuard @" << this << " has no page." << std::endl;
  }
}
WritePageGuard::WritePageGuard(BasicPageGuard &&basic) : guard_(std::move(basic)) {}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (&that == this) {
    // std::cerr << "Warn: assign function was called on a WritePageGuard Object @" << this
    //           << " which the other object is self." << std::endl;
    return *this;
  }
  if (this->guard_.page_ == nullptr) {
    // std::cerr << "Warn: WritePageGuard @" << this << " has no page." << std::endl;
  } else {
    this->Drop();
  }
  this->guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (this->guard_.page_ != nullptr) {
    this->guard_.page_->WUnlatch();
  }
  this->guard_.Drop();
}

WritePageGuard::~WritePageGuard() { this->Drop(); }

auto WritePageGuard::PageId() -> page_id_t { return this->guard_.PageId(); }
auto WritePageGuard::GetData() -> const char * { return this->guard_.GetData(); }
// NOLINT

}  // namespace bustub
