
INCLUDE=-I../../../../tera/thirdparty/boost_1_57_0 \
		-I./gtest/include \
		-I../../ -I../../../thirdparty/leveldb/include \
		-I../../../thirdparty/include
#CXX=/opt/compiler/gcc-4.8.2/bin/g++
namespace_test: namespace_test.cc ../namespace.cc ../../proto/file.pb.cc ../../flags.cc
	$(CXX) -g -o $@ $^ ../../common/logging.cc $(INCLUDE) -L./gtest/lib/ -lgtest -lgflags -L../../../thirdparty/lib -lprotobuf -L../../../thirdparty/leveldb/ -lleveldb -lpthread -lz

clean:
	rm -rf namespace_test *.o
