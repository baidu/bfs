
all: sliding_window_test thread_pool_test

sliding_window_test: sliding_window.h sliding_window_test.cc
	g++ -g -o sliding_window_test sliding_window_test.cc

thread_pool_test: thread_pool_test.cc thread_pool.h
	g++ -g -o thread_pool_test thread_pool_test.cc -lpthread

clean:
	rm -rf sliding_window_test
	rm -rf thread_pool_test
