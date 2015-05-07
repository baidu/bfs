
OPT ?= -g2 -Wall

INCLUDE_PATH= -I../output/include

CXXFLAGS += $(OPT)

LDFLAGS=-L../output/lib -lbfs

all: bfs_wrapper.so

bfs_wrapper.so: bfs_wrapper.cc bfs_wrapper.h
	g++ -rdynamic -g -Wall -shared -fPIC $(INCLUDE_PATH) $(LDFLAGS) -o $@ bfs_wrapper.cc \
		-Xlinker "-(" ../libbfs.a \
		../thirdparty/leveldb/libleveldb.a \
		../../../../third-64/gflags/lib/libgflags.a \
		../../../../third-64/protobuf/lib/libprotobuf.a \
		../../../../third-64/snappy/lib/libsnappy.a \
		-Xlinker "-)"
	
		#../../../../public/sofa-pbrpc/output/lib/libsofa-pbrpc.a \

clean:
	rm -rf bfs_wrapper.so
