rebuild:
	mkdir -p build
	cd build && cmake .. && make

.PHONY: rebuild

fc_tests: rebuild
	cd build && ./fc_tests

fc_benchmarks: rebuild
	cd build && ./fc_benchmarks

db_tests: rebuild
	cd build && ./db_tests

db_tests_debug: rebuild
	cd build && lldb -- ./db_tests