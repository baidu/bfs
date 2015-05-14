
OPT ?= -g2 -Wall

INCLUDE_PATH= -I../output/include -I../src/

CXXFLAGS += $(OPT)

LDFLAGS = -L../output/lib -lbfs
TESTLDFLAGS = -rdynamic -lprotobuf -lpthread -lz -ldl
#-L../../../../third-64/protobuf/lib/

all: bfs_wrapper.so bfs_test

bfs_wrapper.so: bfs_wrapper.cc bfs_wrapper.h ../output/lib/libbfs.a
	g++ $(CXXFLAGS) -shared -fPIC $(INCLUDE_PATH) $(LDFLAGS) -o $@ bfs_wrapper.cc ../src/common/logging.cc \
		-Xlinker "-(" ../libbfs.a \
		-Xlinker "-)"
		#../../../../third-64/protobuf/lib/libprotobuf.a \
		#../../../../public/sofa-pbrpc/output/lib/libsofa-pbrpc.a \
		#../../../../third-64/snappy/lib/libsnappy.a \
		#../../../../third-64/gflags/lib/libgflags.a \
	
bfs_test: bfs_test.cc bfs_wrapper.so
	g++ $(CXXFLAGS) -o $@ bfs_test.cc $(TESTLDFLAGS)

clean:
	rm -rf bfs_wrapper.so
	rm bfs_test
