
# OPT ?= -O2 -DNDEBUG # (A) Production use (optimized mode)
OPT ?= -g2 # (B) Debug mode, w/ full line-level debugging symbols
# OPT ?= -O2 -g2 -DNDEBUG # (C) Profiling mode: opt, but w/debugging symbols

include depends.mk
#CXX=/opt/compiler/gcc-4.8.2/bin/g++

INCLUDE_PATH = -I./src -I$(PROTOBUF_PATH)/include \
               -I$(PBRPC_PATH)/include \
               -I./thirdparty/leveldb/include \
               -I$(SNAPPY_PATH)/include \
               -I$(BOOST_PATH) \
               -I$(GFLAG_PATH)/include \
               -I$(COMMON_PATH)/include

LDFLAGS = -L$(PBRPC_PATH)/lib -lsofa-pbrpc \
          -L$(PROTOBUF_PATH)/lib -lprotobuf \
          -L./thirdparty/leveldb -lleveldb \
          -L$(SNAPPY_PATH)/lib -lsnappy \
          -L$(GFLAG_PATH)/lib -lgflags \
          -L$(GTEST_PATH)/lib -lgtest \
          -L$(TCMALLOC_PATH)/lib -ltcmalloc_minimal \
          -L$(COMMON_PATH)/lib -lcommon -lpthread -lz -lrt

CXXFLAGS = -Wall -fPIC $(OPT)
FUSEFLAGS = -D_FILE_OFFSET_BITS=64 -DFUSE_USE_VERSION=26 -I$(FUSE_PATH)/include

PROTO_FILE = $(wildcard src/proto/*.proto)
PROTO_SRC = $(patsubst %.proto,%.pb.cc,$(PROTO_FILE))
PROTO_HEADER = $(patsubst %.proto,%.pb.h,$(PROTO_FILE))
PROTO_OBJ = $(patsubst %.proto,%.pb.o,$(PROTO_FILE))

NAMESERVER_SRC = $(wildcard src/nameserver/*.cc)
NAMESERVER_OBJ = $(patsubst %.cc, %.o, $(NAMESERVER_SRC))
NAMESERVER_HEADER = $(wildcard src/nameserver/*.h)

CHUNKSERVER_SRC = $(wildcard src/chunkserver/*.cc)
CHUNKSERVER_OBJ = $(patsubst %.cc, %.o, $(CHUNKSERVER_SRC))
CHUNKSERVER_HEADER = $(wildcard src/chunkserver/*.h)

SDK_SRC = $(wildcard src/sdk/*.cc)
SDK_OBJ = $(patsubst %.cc, %.o, $(SDK_SRC))
SDK_HEADER = $(wildcard src/sdk/*.h)

FUSE_SRC = $(wildcard fuse/*.cc)
FUSE_OBJ = $(patsubst %.cc, %.o, $(FUSE_SRC))
FUSE_HEADER = $(wildcard fuse/*.h)

CLIENT_OBJ = $(patsubst %.cc, %.o, $(wildcard src/client/*.cc))

LEVELDB = ./thirdparty/leveldb/libleveldb.a

COMMON_OBJ = $(patsubst %.cc, %.o, $(wildcard src/common/*.cc))
FLAGS_OBJ = src/flags.o
VERSION_OBJ = src/version.o
OBJS = $(FLAGS_OBJ) $(COMMON_OBJ) $(PROTO_OBJ) $(VERSION_OBJ)

LIBS = libbfs.a
BIN = nameserver chunkserver bfs_client

ifdef FUSE_PATH
	BIN += bfs_mount
endif

TESTS = namespace_test file_cache_test chunkserver_impl_test location_provider_test 
TEST_OBJS = src/nameserver/test/namespace_test.o src/chunkserver/test/file_cache_test.o \
			src/chunkserver/test/chunkserver_impl_test.o src/nameserver/test/location_provider_test.o
UNITTEST_OUTPUT = test/

all: $(BIN)
	@echo 'Done'

# Depends
$(NAMESERVER_OBJ) $(CHUNKSERVER_OBJ) $(PROTO_OBJ) $(SDK_OBJ): $(PROTO_HEADER)
$(NAMESERVER_OBJ): $(NAMESERVER_HEADER)
$(CHUNKSERVER_OBJ): $(CHUNKSERVER_HEADER)
$(SDK_OBJ): $(SDK_HEADER)
$(FUSE_OBJ): $(FUSE_HEADER)

# Targets

check: all $(TESTS)
	mkdir -p $(UNITTEST_OUTPUT)
	mv $(TESTS) $(UNITTEST_OUTPUT)
	cd $(UNITTEST_OUTPUT); for t in $(TESTS); do echo "***** Running $$t"; ./$$t || exit 1; done

namespace_test: src/nameserver/test/namespace_test.o
	$(CXX) src/nameserver/namespace.o src/nameserver/test/namespace_test.o $(OBJS) -o $@ $(LDFLAGS)

file_cache_test: src/chunkserver/test/file_cache_test.o
	$(CXX) src/chunkserver/file_cache.o src/chunkserver/test/file_cache_test.o $(OBJS) -o $@ $(LDFLAGS)

chunkserver_impl_test: src/chunkserver/test/chunkserver_impl_test.o \
	src/chunkserver/chunkserver_impl.o src/chunkserver/data_block.o src/chunkserver/block_manager.o \
	src/chunkserver/counter_manager.o src/chunkserver/file_cache.o
	$(CXX) $^ $(OBJS) -o $@ $(LDFLAGS)

location_provider_test: src/nameserver/test/location_provider_test.o src/nameserver/location_provider.o
	$(CXX) $^ $(OBJS) -o $@ $(LDFLAGS)

nameserver: $(NAMESERVER_OBJ) $(OBJS) $(LEVELDB)
	$(CXX) $(NAMESERVER_OBJ) $(OBJS) -o $@ $(LDFLAGS)

chunkserver: $(CHUNKSERVER_OBJ) $(OBJS) $(LEVELDB)
	$(CXX) $(CHUNKSERVER_OBJ) $(OBJS) -o $@ $(LDFLAGS)

libbfs.a: $(SDK_OBJ) $(OBJS) $(PROTO_HEADER)
	$(AR) -rs $@ $(SDK_OBJ) $(OBJS)

bfs_client: $(CLIENT_OBJ) $(LIBS) $(LEVELDB)
	$(CXX) $(CLIENT_OBJ) $(LIBS) -o $@ $(LDFLAGS)

bfs_mount: $(FUSE_OBJ) $(LIBS)
	$(CXX) $(FUSE_OBJ) $(LIBS) -o $@ -L$(FUSE_PATH)/lib -Wl,-static -lfuse -Wl,-call_shared -ldl $(LDFLAGS) 
$(LEVELDB):
	cd thirdparty/leveldb; make -j4

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(INCLUDE_PATH) -c $< -o $@
$(FUSE_OBJ): %.o: %.cc
	$(CXX) $(CXXFLAGS) $(FUSEFLAGS) $(INCLUDE_PATH) -c $< -o $@

%.pb.h %.pb.cc: %.proto
	$(PROTOC) --proto_path=./src/proto/ --proto_path=/usr/local/include --cpp_out=./src/proto/ $<
src/version.cc: FORCE
	sh build_version.sh

.PHONY: FORCE
FORCE:

clean:
	rm -rf $(BIN)
	rm -rf $(NAMESERVER_OBJ) $(CHUNKSERVER_OBJ) $(SDK_OBJ) $(CLIENT_OBJ) $(OBJS) $(TEST_OBJS)
	rm -rf $(PROTO_SRC) $(PROTO_HEADER)
	rm -rf $(UNITTEST_OUTPUT)

install:
	rm -rf output
	mkdir -p output/include
	mkdir -p output/lib
	mkdir -p output/bin
	cp -f src/sdk/bfs.h output/include/
	cp -f libbfs.a output/lib/
	cp -f bfs_client output/bin/
	touch output/bfs.flag

.PHONY: test
test:
	cd sandbox; sh small_test.sh

