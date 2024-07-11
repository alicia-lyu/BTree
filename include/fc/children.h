#ifndef CHILDREN_H
#define CHILDREN_H

#include <array>
#include <concepts>
#include <cstddef>
#include <iterator>
#include <memory>
#include <variant>
#include <vector>
#include "fc/details.h"

namespace children {

using namespace frozenca;

template <class Node, class Deleter>
class Child : public std::variant<std::unique_ptr<Node, Deleter>, size_t> {
 public:
  Node *get() {
    if (std::holds_alternative<std::unique_ptr<Node, Deleter>>(*this)) {
      return std::get<std::unique_ptr<Node, Deleter>>(*this).get();
    } else {
      throw std::runtime_error("Cannot get for size_t");
    }
  }
};

template <class Node, class Deleter>
using children_nodes_type = std::vector<std::unique_ptr<Node, Deleter>>;
template <attr_t disk_max_nkeys>
using children_data_type = std::array<size_t, disk_max_nkeys>;

template <class Node, class Deleter, attr_t disk_max_nkeys>
struct ChildrenIterTraits {
  using difference_type = attr_t;
  using value_type = std::variant<std::unique_ptr<Node, Deleter>, size_t>;
  using pointer = value_type *;
  using reference = value_type &;
  using iterator_category = std::bidirectional_iterator_tag;
  using iterator_concept = iterator_category;

  using nodes_iter = typename children_nodes_type<Node, Deleter>::iterator;
  using data_iter = typename children_data_type<disk_max_nkeys>::iterator;
};

template <class Node, class Deleter, attr_t disk_max_nkeys>
struct ChildrenConstIterTraits {
  using difference_type = attr_t;
  using value_type = std::variant<std::unique_ptr<Node, Deleter>, size_t>;
  using pointer = const value_type *;
  using reference = const value_type &;
  using iterator_category = std::bidirectional_iterator_tag;
  using iterator_concept = iterator_category;

  using nodes_iter = typename children_nodes_type<Node, Deleter>::const_iterator;
  using data_iter = typename children_data_type<disk_max_nkeys>::const_iterator;
};

template <typename IterTraits>
class ChildrenIterator {
 public:
  using value_type = typename IterTraits::value_type;
  using difference_type = typename IterTraits::difference_type;
  using pointer = typename IterTraits::pointer;
  using reference = typename IterTraits::reference;
  using iterator_category = typename IterTraits::iterator_category;
  using iterator_concept = typename IterTraits::iterator_concept;

 private:
  using nodes_iter = typename IterTraits::nodes_iter;
  using data_iter = typename IterTraits::data_iter;
  std::variant<nodes_iter, data_iter> iter_;

 public:
  explicit ChildrenIterator(std::variant<nodes_iter, data_iter> iter) : iter_(iter) {}

  ChildrenIterator() {
    iter_ = nodes_iter();
  }

  ChildrenIterator &operator++() {
    return std::visit([](auto &i) { return ++i; }, iter_);
  }

  ChildrenIterator operator++(int) {
    return std::visit([](auto &i) { return i++; }, iter_);
  }

  ChildrenIterator &operator--() {
    return std::visit([](auto &i) { return --i; }, iter_);
  }

  reference operator*() {
    return std::visit([](auto &i) -> reference { return *i; }, iter_);
  }

  pointer operator->() {
    return std::visit([](auto &i) -> pointer { return &*i; }, iter_);
  }

  bool operator==(const ChildrenIterator &other) const { return iter_ == other.iter_; }

  bool operator!=(const ChildrenIterator &other) const { return iter_ != other.iter_; }
};

template <class Node, class Deleter, attr_t disk_max_nkeys>
class Children {
  using nodes_type = children_nodes_type<Node, Deleter>;
  using data_type = children_data_type<disk_max_nkeys>;
  using child_type = Child<Node, Deleter>;

  std::variant<nodes_type, data_type> data_;

 public:
  using iterator = ChildrenIterator<ChildrenIterTraits<Node, Deleter, disk_max_nkeys>>;
  using const_iterator = ChildrenIterator<ChildrenConstIterTraits<Node, Deleter, disk_max_nkeys>>;
  using value_type = typename iterator::value_type;

  Children() {
    data_ = nodes_type();
  }

  void reserve(size_t n) {
    if (std::holds_alternative<nodes_type>(data_)) {
      std::get<nodes_type>(data_).reserve(n);
    } else {
      throw std::runtime_error("Cannot reserve for children_data_type");
    }
  }

  void push_back(std::unique_ptr<Node, Deleter> &&node) {
    if (std::holds_alternative<nodes_type>(data_)) {
      std::get<nodes_type>(data_).push_back(std::move(node));
    } else {
      throw std::runtime_error("Cannot push_back for children_data_type");
    }
  }

  child_type &operator[](size_t idx) {
    if (std::holds_alternative<nodes_type>(data_)) {
      return std::get<nodes_type>(data_)[idx];
    } else {
      return std::get<data_type>(data_)[idx];
    }
  }

  bool empty() const {
    if (std::holds_alternative<nodes_type>(data_)) {
      return std::get<nodes_type>(data_).empty();
    } else {
      for (auto &&child : std::get<data_type>(data_)) {
        if (child != 0) {
          return false;
        }
      }
      return true;
    }
  }

  iterator begin() noexcept {
    return std::visit([](auto &children) { return iterator(children.begin()); }, data_);
  }

  iterator end() noexcept {
    return std::visit([](auto &children) { return iterator(children.end()); }, data_);
  }

  const_iterator begin() const noexcept {
    return std::visit([](const auto &children) { return const_iterator(children.cbegin()); }, data_);
  }

  const_iterator end() const noexcept {
    return std::visit([](const auto &children) { return const_iterator(children.cend()); }, data_);
  }

  const_iterator cbegin() const noexcept { return begin(); }

  const_iterator cend() const noexcept { return end(); }
};

}  // namespace children

#endif
