
INCLUDE=-I../../ \
		-I../../../thirdparty/boost_1_57_0 \
		-I../../../thirdparty/leveldb/include \
		-I../../../thirdparty/include
#CXX=/opt/compiler/gcc-4.8.2/bin/g++
namespace_test: namespace_test.cc ../namespace.cc ../../proto/file.pb.cc ../../flags.cc
	$(CXX) -g -o $@ $^ $(INCLUDE) -L./gtest/lib/ -lcommon -lgtest -lgflags -L../../../thirdparty/lib -lprotobuf -L../../../thirdparty/leveldb/ -lleveldb -lpthread -lz

clean:
	rm -rf namespace_test *.o

#-I./gtest/include \
