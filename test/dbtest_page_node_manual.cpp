#include "fc/btree.h"
#include <cassert>
#include <iostream>
#include <random>
#include <vector>
#include <numeric>

int main() {
  namespace fc = frozenca;
  // BTree insert-lookup test
  {
    fc::BTreeSet<int> btree;
    constexpr int n = 100;

    std::mt19937 gen(std::random_device{}());

    std::vector<int> v(n);
    std::iota(v.begin(), v.end(), 0);

    // Random insert
    {
      std::ranges::shuffle(v, gen);
      for (auto num : v) {
        auto page_res = btree.insert_page(num, static_cast<size_t>(num));  // Assuming page_index is just num for simplicity
        assert(page_res.second);
      }
    }

    // Random lookup
    {
      std::ranges::shuffle(v, gen);
      for (auto num : v) {
        auto page_res = btree.find_page(num);
        assert(page_res.second != nullptr);
      }
    }
  }

  // BTree std::initializer_list test
  {
    fc::BTreeSet<int> btree{1, 4, 3, 2, 3, 3, 6, 5, 8};
    assert(btree.size() == 7);
  }

  // Multiset test
  {
    fc::BTreeMultiSet<int> btree{1, 4, 3, 2, 3, 3, 6, 5, 8};
    assert(btree.size() == 9);
    // btree.erase(3);
    assert(btree.size() == 6);
  }

  // Order statistic test
  {
    fc::BTreeSet<int> btree;
    constexpr int n = 100;

    for (int i = 0; i < n; ++i) {
      btree.insert_page(i, i);
    }

    for (int i = 0; i < n; ++i) {
      assert(btree.kth(i) == i);
    }

    for (int i = 0; i < n; ++i) {
      assert(btree.order(btree.find(i)) == i);
    }
  }

  // Enumerate test
  {
    fc::BTreeSet<int> btree;
    constexpr int n = 100;

    for (int i = 0; i < n; ++i) {
      btree.insert_page(i, i);
    }
    auto rg = btree.enumerate(20, 30);
    assert(std::ranges::distance(rg.begin(), rg.end()) == 11);

    // // erase_if test
    // {
    //   btree.erase_if([](auto n) { return n >= 20 && n <= 90; });
    //   assert(btree.size() == 29);
    // }
  }

  // Join/Split test
  {
    fc::BTreeSet<int> btree1;
    for (int i = 0; i < 100; ++i) {
      btree1.insert_page(i, i);
    }

    fc::BTreeSet<int> btree2;
    for (int i = 101; i < 300; ++i) {
      btree2.insert_page(i, i);
    }
    fc::BTreeSet<int> btree3;

    btree3 = fc::join(std::move(btree1), 100, std::move(btree2));

    for (int i = 0; i < 300; ++i) {
      assert(btree3.contains(i));
    }

    auto [btree4, btree5] = fc::split(std::move(btree3), 200);
    for (int i = 0; i < 200; ++i) {
      assert(btree4.contains(i));
    }
    assert(!btree5.contains(200));

    for (int i = 201; i < 300; ++i) {
      assert(btree5.contains(i));
    }
  }

  // Multiset split test
  {
    fc::BTreeMultiSet<int> btree6;
    btree6.insert(0);
    btree6.insert(2);
    for (int i = 0; i < 100; ++i) {
      btree6.insert(1);
    }
    auto [btree7, btree8] = fc::split(std::move(btree6), 1);
    assert(btree7.size() == 1);
    assert(btree8.size() == 1);
  }

  // Two arguments join test
  {
    fc::BTreeSet<int> tree1;
    for (int i = 0; i < 100; ++i) {
      tree1.insert(i);
    }
    fc::BTreeSet<int> tree2;
    for (int i = 100; i < 200; ++i) {
      tree2.insert(i);
    }
    auto tree3 = fc::join(std::move(tree1), std::move(tree2));
    for (int i = 0; i < 200; ++i) {
      assert(tree3.contains(i));
    }
  }

  // Three arguments split test
  {
    fc::BTreeSet<int> tree1;
    for (int i = 0; i < 100; ++i) {
      tree1.insert(i);
    }
    auto [tree2, tree3] = fc::split(std::move(tree1), 10, 80);
    assert(tree2.size() == 10);
    assert(tree3.size() == 19);
  }

  // Multiset erase test
  {
    fc::BTreeMultiSet<int> tree1;
    tree1.insert(0);
    for (int i = 0; i < 100; ++i) {
      tree1.insert(1);
    }
    tree1.insert(2);

    tree1.erase(1);

    assert(tree1.size() == 2);
  }

  // Range insert-1 test
  {
    fc::BTreeSet<int> btree;
    btree.insert(1);
    btree.insert(10);

    std::vector<int> v{2, 5, 4, 3, 7, 6, 6, 6, 2, 8, 8, 9};
    btree.insert_range(std::move(v));

    for (int i = 1; i < 10; ++i) {
      assert(btree.contains(i));
    }
  }

  // Range insert-2 test
  {
    fc::BTreeSet<int> btree;
    btree.insert(1);
    btree.insert(10);

    std::vector<int> v{2, 5, 4, 3, 7, 6, 6, 6, 2, 8, 8, 9, 10};
    btree.insert_range(std::move(v));

    for (int i = 1; i < 10; ++i) {
      assert(btree.contains(i));
    }
  }

  // count() test
  {
    fc::BTreeMultiSet<int> btree2;
    btree2.insert(1);
    btree2.insert(1);
    assert(btree2.count(1) == 2);
    assert(btree2.count(0) == 0);
    assert(btree2.count(2) == 0);
  }

  std::cout << "All tests passed!" << std::endl;
  return 0;
}
