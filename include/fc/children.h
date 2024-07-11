#ifndef CHILDREN_H
#define CHILDREN_H

#include <array>
#include <cstddef>
#include <iterator>
#include <memory>
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

 private:
  using nodes_iter = typename IterTraits::nodes_iter;
  using data_iter = typename IterTraits::data_iter;
  std::variant<nodes_iter, data_iter> iter_;

  explicit ChildrenIterator(std::variant<nodes_iter, data_iter> iter) : iter_(iter) {}

  explicit ChildrenIterator(nodes_iter iter) : iter_(iter) {}

  explicit ChildrenIterator(data_iter iter) : iter_(iter) {}

  ChildrenIterator &operator++() {
    std::visit([](auto &i) { ++i; }, iter_);
    return *this;
  }

  ChildrenIterator &operator--() {
    std::visit([](auto &i) { --i; }, iter_);
    return *this;
  }

  value_type &operator*() {
    return std::visit([](auto &i) -> value_type & { return *i; }, iter_);
  }

  value_type *operator->() {
    return std::visit([](auto &i) -> value_type * { return &*i; }, iter_);
  }

  bool operator==(const ChildrenIterator &other) const { return iter_ == other.iter_; }

  bool operator!=(const ChildrenIterator &other) const { return iter_ != other.iter_; }
};

template <class Node, class Deleter, attr_t disk_max_nkeys>
class Children : public std::variant<children_nodes_type<Node, Deleter>, children_data_type<disk_max_nkeys>> {

  using nodes_type = children_nodes_type<Node, Deleter>;
  using data_type = children_data_type<disk_max_nkeys>;
  using child_type = Child<Node, Deleter>;

 public:
  using iterator = ChildrenIterator<ChildrenIterTraits<Node, Deleter, disk_max_nkeys>>;
  using const_iterator = ChildrenIterator<ChildrenConstIterTraits<Node, Deleter, disk_max_nkeys>>;

  void reserve(size_t n) {
    if (std::holds_alternative<nodes_type>(*this)) {
      std::get<nodes_type>(*this).reserve(n);
    } else {
      throw std::runtime_error("Cannot reserve for children_data_type");
    }
  }

  void push_back(std::unique_ptr<Node, Deleter> &&node) {
    if (std::holds_alternative<nodes_type>(*this)) {
      std::get<nodes_type>(*this).push_back(std::move(node));
    } else {
      throw std::runtime_error("Cannot push_back for children_data_type");
    }
  }

  child_type &operator[](size_t idx) {
    if (std::holds_alternative<nodes_type>(*this)) {
      return std::get<nodes_type>(*this)[idx];
    } else {
      return std::get<data_type>(*this)[idx];
    }
  }

  bool empty() const {
    if (std::holds_alternative<nodes_type>(*this)) {
      return std::get<nodes_type>(*this).empty();
    } else {
      for (auto &&child : std::get<data_type>(*this)) {
        if (child != 0) {
          return false;
        }
      }
      return true;
    }
  }

  // For std::ranges::move
  iterator begin() {
    return std::visit([](auto &children) { return iterator(children.begin()); }, *this);
  }

  iterator end() {
    return std::visit([](auto &children) { return iterator(children.end()); }, *this);
  }

  const_iterator begin() const {
    return std::visit([](auto &children) { return const_iterator(children.cbegin()); }, *this);
  }

  const_iterator end() const {
    return std::visit([](auto &children) { return const_iterator(children.cend()); }, *this);
  }
};
}  // namespace children

#endif