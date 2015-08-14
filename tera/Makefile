
OPT ?= -g2 -Wall

INCLUDE_PATH= -I../output/include -I../src/

CXXFLAGS += $(OPT)

TESTLDFLAGS = -rdynamic -lprotobuf -lpthread -lz -ldl -L../../../../third-64/protobuf/lib/

all: bfs_wrapper.so bfs_test

bfs_wrapper.so: bfs_wrapper.cc bfs_wrapper.h ../output/lib/libbfs.a
	g++ $(CXXFLAGS) -shared -fPIC $(INCLUDE_PATH) -o $@ bfs_wrapper.cc ../src/common/logging.cc \
		-Xlinker "-(" ../libbfs.a \
		../thirdparty/ins/output/lib/libins_sdk.a \
		../../tera/thirdparty/lib/libprotobuf.a \
		../../tera/thirdparty/lib/libsofa-pbrpc.a \
		../../tera/thirdparty/lib/libsnappy.a \
		../../tera/thirdparty/lib/libgflags.a \
		-lpthread \
		-Xlinker "-)"

	
bfs_test: bfs_test.cc bfs_wrapper.so
	g++ $(CXXFLAGS) $(INCLUDE_PATH) -o $@ bfs_test.cc $(TESTLDFLAGS)

clean:
	rm -rf bfs_wrapper.so
	rm -rf bfs_test
