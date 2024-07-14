rebuild:
	mkdir -p build
	cd build && cmake .. && make

.PHONY: rebuild

fc_tests: rebuild
	cd build && ./fc_tests

fc_benchmarks: rebuild
	cd build && ./fc_benchmarks

# dbtests_manual_fixed_page: rebuild
# 	cd build && lldb -- ./dbtests_manual_fixed_page

dbtests_manual_page_node: rebuild
	cd build && lldb -- ./dbtests_manual_page_node

dbtests_manual_dbbtree: rebuild
	cd build && lldb -- ./dbtests_manual_dbbtree