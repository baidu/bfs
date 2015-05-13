
OPT ?= -g2 -Wall

INCLUDE_PATH= -I../output/include

CXXFLAGS += $(OPT)

LDFLAGS=-L../output/lib -lbfs
TESTLDFLAGS=-ldl -pthread -lz -lprotobuf -L../../../../third-64/protobuf/lib/

all: bfs_wrapper.so bfs_test

bfs_wrapper.so: bfs_wrapper.cc bfs_wrapper.h ../output/lib/libbfs.a
	g++ $(CXXFLAGS) -shared -fPIC $(INCLUDE_PATH) $(LDFLAGS) -o $@ bfs_wrapper.cc \
		-Xlinker "-(" ../libbfs.a \
		../../../../third-64/protobuf/lib/libprotobuf.a \
		../../../../public/sofa-pbrpc/output/lib/libsofa-pbrpc.a \
		../../../../third-64/snappy/lib/libsnappy.a \
		../../../../third-64/gflags/lib/libgflags.a \
		-Xlinker "-)"
	
bfs_test: bfs_test.cc bfs_wrapper.so
	g++ $(CXXFLAGS) $(TESTLDFLAGS) -o $@ bfs_test.cc

clean:
	rm -rf bfs_wrapper.so
	rm bfs_test
