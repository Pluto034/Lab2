#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  //  throw NotImplementedException("Trie::Get is not implemented.");

  auto cur = root_.get();
  if (cur == nullptr) {
    return nullptr;
  }
  for (auto ch : key) {
    if (cur->children_.count(ch) == 0U) {
      return nullptr;
    }
    cur = cur->children_.at(ch).get();
  }
  auto end = dynamic_cast<const TrieNodeWithValue<T> *>(cur);
  if (end == nullptr || (not end->is_value_node_)) {
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
  //  throw NotImplementedException("Trie::Put is not implemented.");
  auto len = key.length();
  std::function<std::shared_ptr<TrieNode>(const TrieNode *, size_t)> add =
      [&](const TrieNode *old, size_t pos) -> std::shared_ptr<TrieNode> {
    if (pos == len) {
      if (old == nullptr) {
        return std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
      }
      return std::make_shared<TrieNodeWithValue<T>>(old->children_, std::make_shared<T>(std::move(value)));
    }

    char ch = key[pos];
    std::shared_ptr<const TrieNode> n_node;

    if (old != nullptr && old->children_.count(ch)) {
      n_node = add(old->children_.at(ch).get(), pos + 1);
    } else {
      n_node = add(nullptr, pos + 1);
    }
    // nullptr need to new build
    if (old == nullptr) {
      std::map<char, std::shared_ptr<const TrieNode>> mp;
      mp.emplace(ch, n_node);
      return std::make_shared<TrieNode>(mp);
    }
    auto mp = old->Clone();
    mp->children_.erase(ch);
    mp->children_.emplace(ch, n_node);
    return mp;
  };
  auto n_node = add(root_.get(), 0);
  Trie *end = new Trie(n_node);
  return *end;

  //  add(root_.get(), 0);  // return a TrieNode shared ptr
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie
{
  //    throw NotImplementedException("Trie::Remove is not implemented.");
  auto len = key.length();
  auto cur = root_.get();
  for (auto ch : key) {
    if (cur->children_.count(ch) == 0U) {
      return *this;
    }
    cur = cur->children_.at(ch).get();
  }
  if (!cur->is_value_node_) {
    return *this;
  }

  std::function<std::shared_ptr<TrieNode>(const TrieNode *, size_t)> del =
      [&](const TrieNode *old, size_t pos) -> std::shared_ptr<TrieNode> {
    if (pos == len) {
      if (old->children_.empty()) {
        return nullptr;
      }
      return std::make_shared<TrieNode>(old->children_);
    }
    char ch = key[pos];
    auto n_node = del(old->children_.at(ch).get(), pos + 1);

    if (n_node == nullptr) {
      if (old->children_.size() == 1 && !old->is_value_node_) {
        return nullptr;
      }
      auto tmp = old->Clone();
      tmp->children_.erase(ch);
      return tmp;
    }
    auto tmp = old->Clone();
    tmp->children_.erase(ch);
    tmp->children_.emplace(ch, std::move(n_node));
    return tmp;
  };
  auto n_node = del(root_.get(), 0);
  Trie *end = new Trie(n_node);
  return *end;

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
