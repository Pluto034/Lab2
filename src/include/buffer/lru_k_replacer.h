//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <forward_list>
#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

#include <iostream>

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

using time_t_ = size_t;
using history_t = std::list<time_t_>;

class LRUKNode {
  constexpr static time_t_ N_TIME = static_cast<time_t_>(-1);

 public:
  explicit LRUKNode(frame_id_t fid, size_t k) : k_(k), fid_(fid) {
    // 一开始没有k次访问
    size_ = 0;
    k_last_ = history_.end();
  }

  // 添加一次访问
  auto Access(time_t_ timestamp) -> void {
    size_++;
    history_.push_front(timestamp);
    if (size_ == 1) {
      k_last_ = history_.begin();
    }
    if (size_ <= k_) {
      return;
    }
    k_last_--;
    Clean();
  };

  /**
   * @param cur_time 当前时间戳
   * @param k_dis 返回的k_dis
   *
   * @return 返回是否是无穷
   * */
  auto CalcKDis(time_t_ cur_time, time_t_ &k_dis) const -> bool {
    if (size_ < k_) {
      if (size_ == 0) {
        k_dis = N_TIME;
      } else {
        k_dis = *k_last_;
      }
      return true;
    }
    k_dis = cur_time - *k_last_;
    return false;
  }

 private:
  auto Clean() -> void {
    // 如果小于k的两倍, 不清理
    if (size_ <= k_ * 2) {
      return;
    }
    size_ = k_;
    if (k_last_ != history_.end()) {
      k_last_ = std::prev(history_.erase(std::next(k_last_), history_.end()));
    }
  }

  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.

  // 历史记录的长度(不是所有历史的长度, 历史会定期清理)
  size_t size_;
  // 第k个元素的迭代器, 如果没有k个元素就指向end
  std::list<time_t_>::iterator k_last_;

  size_t k_;
  // 页的编号
  [[maybe_unused]] frame_id_t fid_;
  // 访问的历史记录
  std::list<time_t_> history_;
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'Evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict frame with earliest timestamp
   * based on LRU.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   * @param access_type type of access that was received. This parameter is only needed for
   * leaderboard tests.
   */
  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is Evictable or non-Evictable. This function also
   * controls replacer's size. Note that size is equal to number of Evictable entries.
   *
   * If a frame was previously Evictable and is to be set to non-Evictable, then size should
   * decrement. If a frame was previously non-Evictable and is to be set to Evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'Evictable' status will be modified
   * @param set_evictable whether the given frame is Evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an Evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-Evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of Evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  std::unordered_map<frame_id_t, LRUKNode> node_evict_, node_store_;
  size_t current_timestamp_{0};
  [[maybe_unused]] size_t replacer_size_;
  size_t k_;
  std::mutex latch_;
  //  [[maybe_unused]] size_t curr_size_{0};
};

}  // namespace bustub
