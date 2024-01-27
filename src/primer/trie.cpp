#include "primer/trie.h"
#include <memory>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  auto cur = GetRoot().get();

  if (cur == nullptr) {
    return nullptr;
  }
  for (const auto ch : key) {
    if (not cur->children_.count(ch)) {
      return nullptr;
    }
    cur = cur->children_.at(ch).get();
  }

  auto end = dynamic_cast<const TrieNodeWithValue<T> *>(cur);
  if ((end == nullptr) or (not cur->is_value_node_)) {
    return nullptr;
  }

  return end->value_.get();

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  using strBase = std::string_view::value_type;

  auto len = key.size();
  std::function<std::unique_ptr<const TrieNode>(const TrieNode *, size_t)> add =
      [&](const TrieNode *old, size_t pos) -> std::unique_ptr<const TrieNode> {
    if (pos == len) {
      if (old == nullptr) {
        return std::make_unique<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
      }
      return std::make_unique<TrieNodeWithValue<T>>(old->children_, std::make_shared<T>(std::move(value)));
    }
    strBase ch = key.at(pos);
    std::unique_ptr<const TrieNode> n_child;
    // 如果有这个分支
    if (old != nullptr and old->children_.count(ch)) {
      n_child = add(old->children_.at(ch).get(), pos + 1);
    } else {
      n_child = add(nullptr, pos + 1);
    }

    if (old != nullptr) {
      auto tmp = old->Clone();
      if (tmp->children_.count(ch)) {
        tmp->children_.erase(ch);
      }
      tmp->children_.emplace(ch, std::move(n_child));
      return tmp;
    }

    decltype(old->children_) mp;
    mp.emplace(ch, std::move(n_child));
    return std::make_unique<TrieNode>(mp);
  };

  std::unique_ptr<const TrieNode> n_root = add(root_.get(), 0);
  return Trie(std::shared_ptr<const TrieNode>(std::move(n_root)));
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");
  auto len = key.length();
  auto cur = GetRoot().get();

  bool is_end = true;
  for (auto ch : key) {
    if (cur->children_.count(ch) == 0U) {
      is_end = false;
      break;
    }
    cur = cur->children_.at(ch).get();
  }

  // 键或值不存在打情况
  if (not is_end or not cur->is_value_node_) {
    return *this;
  }

  std::function<std::unique_ptr<const TrieNode>(const TrieNode *, size_t)> rm =
      [&](const TrieNode *node, size_t pos) -> std::unique_ptr<TrieNode> {
    if (pos == len) {
      if (node->children_.empty()) {
        return nullptr;
      }
      return std::make_unique<TrieNode>(node->children_);
    }

    auto ch = key.at(pos);
    auto n_node = rm(node->children_.at(ch).get(), pos + 1);

    // 如果下一个节点被删除的话
    if (n_node == nullptr) {
      // 如果这个节点只有一个孩子，并且这个节点没有值，就删除
      if (node->children_.size() == 1 and not node->is_value_node_) {
        return nullptr;
      }
      // 否则说明这个节点还有利用价值
      auto ret = node->Clone();
      ret->children_.erase(ch);
      return ret;
    }

    // 否则说吗这个节点的孩子需要修改
    auto ret = node->Clone();
    ret->children_.erase(ch);
    ret->children_.emplace(ch, std::move(n_node));
    return ret;
  };

  auto n_root = rm(GetRoot().get(), 0);
  return Trie(std::move(n_root));

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
