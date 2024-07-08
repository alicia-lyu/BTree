rebuild:
	mkdir -p build
	cd build && cmake .. && make

.PHONY: rebuild

fc_tests: rebuild
	cd build && ./fc_tests

fc_benchmarks: rebuild
	cd build && ./fc_benchmarks

dbtests_debug: rebuild
	cd build && lldb -- ./dbtests_manual