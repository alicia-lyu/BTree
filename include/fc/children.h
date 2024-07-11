#ifndef CHILDREN_H
#define CHILDREN_H

#include <array>
#include <cstddef>
#include <iterator>
#include <limits>
#include <memory>
#include <variant>
#include <vector>
#include "fc/details.h"

namespace children {

using namespace frozenca;

template <typename Node, typename Deleter>
class ChildProxy {
 private:
  std::variant<std::reference_wrapper<std::unique_ptr<Node, Deleter>>, std::reference_wrapper<size_t>> ref_;

 public:
  ChildProxy(std::unique_ptr<Node, Deleter>& node) : ref_(node) {}
  ChildProxy(size_t& idx) : ref_(idx) {}

  Node* get() {
    if (std::holds_alternative<std::reference_wrapper<std::unique_ptr<Node, Deleter>>>(ref_)) {
      return std::get<std::reference_wrapper<std::unique_ptr<Node, Deleter>>>(ref_).get().get();
    } else {
      throw std::runtime_error("Cannot get for size_t");
    }
  }

  Node* operator->() { return get(); }

  operator std::variant<std::unique_ptr<Node, Deleter>, size_t>& () {
    if (std::holds_alternative<std::reference_wrapper<std::unique_ptr<Node, Deleter>>>(ref_)) {
      return std::get<std::reference_wrapper<std::unique_ptr<Node, Deleter>>>(ref_).get();
    } else {
      return std::get<std::reference_wrapper<size_t>>(ref_).get();
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
  using pointer = value_type*;
  using reference = ChildProxy<Node, Deleter>;
  using iterator_category = std::bidirectional_iterator_tag;
  using iterator_concept = iterator_category;

  using nodes_iter = typename children_nodes_type<Node, Deleter>::iterator;
  using data_iter = typename children_data_type<disk_max_nkeys>::iterator;
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

  ChildrenIterator() : iter_(nodes_iter()) {}

  ChildrenIterator(const ChildrenIterator &other) : iter_(other.iter_) {}

  ChildrenIterator &operator++() {
    std::visit([](auto &i) { ++i; }, iter_);
    return *this;
  }

  ChildrenIterator operator++(int) {
    ChildrenIterator tmp(*this);
    ++(*this);
    return tmp;
  }

  ChildrenIterator &operator--() {
    std::visit([](auto &i) { --i; }, iter_);
    return *this;
  }

  ChildrenIterator operator--(int) {
    ChildrenIterator tmp(*this);
    --(*this);
    return tmp;
  }

  ChildrenIterator operator+(difference_type n) const {
    return std::visit([n](auto &i) { return ChildrenIterator(i + n); }, iter_);
  }

  reference operator*() const {
    return std::visit([](auto &i) -> reference {
      return reference(*i);
    }, iter_);
  }

  pointer operator->() const {
    return std::visit([](auto &i) -> pointer {
      return &(*i);
    }, iter_);
  }

  bool operator==(const ChildrenIterator &other) const { return iter_ == other.iter_; }

  bool operator!=(const ChildrenIterator &other) const { return iter_ != other.iter_; }
};

template <class Node, class Deleter, attr_t disk_max_nkeys>
class Children {
  using nodes_type = children_nodes_type<Node, Deleter>;
  using data_type = children_data_type<disk_max_nkeys>;

  std::variant<nodes_type, data_type> data_;

 public:
  using iterator = ChildrenIterator<ChildrenIterTraits<Node, Deleter, disk_max_nkeys>>;

  using value_type = typename iterator::value_type;
  using reference = typename iterator::reference;
  using difference_type = typename iterator::difference_type;
  using size_type = size_t;

  Children() { data_ = nodes_type(); }

  void reserve(size_t n) {
    if (std::holds_alternative<nodes_type>(data_)) {
      std::get<nodes_type>(data_).reserve(n);
    } else {
      throw std::runtime_error("Cannot reserve for children_data_type");
    }
  }

  size_type size() const {
    if (std::holds_alternative<nodes_type>(data_)) {
      return std::get<nodes_type>(data_).size();
    } else {
      size_type count = 0;
      for (auto &&child : std::get<data_type>(data_)) {
        if (child != std::numeric_limits<size_t>::max()) {
          count++;
        }
      }
      return count;
    }
  }

  void push_back(value_type &&child) {
    if (std::holds_alternative<nodes_type>(data_) && std::holds_alternative<std::unique_ptr<Node, Deleter>>(child)) {
      std::get<nodes_type>(data_).push_back(std::move(std::get<std::unique_ptr<Node, Deleter>>(child)));
    } else if (std::holds_alternative<data_type>(data_) && std::holds_alternative<size_t>(child)) {
      auto& arr = std::get<data_type>(data_);
      for (size_t i = 0; i < arr.size(); i++) {
        if (arr[i] == std::numeric_limits<size_t>::max()) {
          arr[i] = std::move(std::get<size_t>(child));
          return;
        }
      }
      throw std::runtime_error("Array full.");
    } else {
      throw std::runtime_error("Cannot push_back for incompatible variants.");
    }
  }

  void push_back(const value_type &child) {
    if (std::holds_alternative<nodes_type>(data_) && std::holds_alternative<std::unique_ptr<Node, Deleter>>(child)) {
      std::get<nodes_type>(data_).push_back(std::move(std::get<std::unique_ptr<Node, Deleter>>(child)));
    } else if (std::holds_alternative<data_type>(data_) && std::holds_alternative<size_t>(child)) {
      auto& arr = std::get<data_type>(data_);
      for (size_t i = 0; i < arr.size(); i++) {
        if (arr[i] == std::numeric_limits<size_t>::max()) {
          arr[i] = std::move(std::get<size_t>(child));
          return;
        }
      }
      throw std::runtime_error("Array full.");
    } else {
      throw std::runtime_error("Cannot push_back for incompatible variants.");
    }
  }

  value_type pop_back() {
    if (std::holds_alternative<nodes_type>(data_)) {
      auto child = std::move(std::get<nodes_type>(data_).back());
      std::get<nodes_type>(data_).pop_back();
      return child;
    } else {
      auto& arr = std::get<data_type>(data_);
      for (size_t i = arr.size(); i-- > 0;) {
        if (arr[i] != std::numeric_limits<size_t>::max()) {
          auto child = arr[i];
          arr[i] = std::numeric_limits<size_t>::max();
          return child;
        }
      }
      throw std::runtime_error("Array empty.");
    }
  }

  void insert(iterator pos, value_type &&child) {
    if (std::holds_alternative<nodes_type>(data_) && std::holds_alternative<std::unique_ptr<Node, Deleter>>(child)) {
      std::get<nodes_type>(data_).insert(std::get<typename iterator::nodes_iter>(pos.iter_), std::move(std::get<std::unique_ptr<Node, Deleter>>(child)));
    } else if (std::holds_alternative<data_type>(data_) && std::holds_alternative<size_t>(child)) {
      auto& arr = std::get<data_type>(data_);
      if (std::get<size_t>(*pos) == std::numeric_limits<size_t>::max()) {
        *pos = child;
        return;
      }
      auto it = std::get<typename iterator::data_iter>(pos.iter_);
      while (it != arr.end() && *it != std::numeric_limits<size_t>::max()) {
        ++it;
      }
      if (it == arr.end()) {
        throw std::runtime_error("Array full.");
      }
      std::move_backward(pos.iter_, it, it + 1);
      *std::get<typename iterator::data_iter>(pos.iter_) = std::get<size_t>(child);
    } else {
      throw std::runtime_error("Cannot insert for incompatible variants.");
    }
  }

  reference operator[](size_t idx) {
    if (std::holds_alternative<nodes_type>(data_)) {
      return ChildProxy<Node, Deleter>(std::get<nodes_type>(data_)[idx]);
    } else {
      return ChildProxy<Node, Deleter>(std::get<data_type>(data_)[idx]);
    }
  }

  bool empty() const {
    if (std::holds_alternative<nodes_type>(data_)) {
      return std::get<nodes_type>(data_).empty();
    } else {
      for (auto &&child : std::get<data_type>(data_)) {
        if (child != std::numeric_limits<size_t>::max()) {
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
};


}  // namespace children

#endif
