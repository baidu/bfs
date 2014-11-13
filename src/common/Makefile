
CXXFLAGS=-g -Wall -O3

all: mutex_test

clean:
	rm -rf mutex_test thread_pool_test

mutex_test: mutex_test.cc mutex.h
	g++ $(CXXFLAGS) -lpthread mutex_test.cc -o mutex_test
