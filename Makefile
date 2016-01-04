
# OPT ?= -O2 -DNDEBUG # (A) Production use (optimized mode)
OPT ?= -g2 # (B) Debug mode, w/ full line-level debugging symbols
# OPT ?= -O2 -g2 -DNDEBUG # (C) Profiling mode: opt, but w/debugging symbols

include depends.mk

INCLUDE_PATH = -I./src -I$(PROTOBUF_PATH)/include \
               -I$(PBRPC_PATH)/include \
               -I./thirdparty/leveldb/include \
               -I$(SNAPPY_PATH)/include \
               -I$(BOOST_PATH)/include \
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

CLIENT_OBJ = $(patsubst %.cc, %.o, $(wildcard src/client/*.cc))

LEVELDB = ./thirdparty/leveldb/libleveldb.a

FLAGS_OBJ = $(patsubst %.cc, %.o, $(wildcard src/*.cc))
COMMON_OBJ = $(patsubst %.cc, %.o, $(wildcard src/common/*.cc))
OBJS = $(FLAGS_OBJ) $(COMMON_OBJ) $(PROTO_OBJ)

LIBS = libbfs.a
BIN = nameserver chunkserver bfs_client

all: $(BIN)
	@echo 'Done'

# Depends
$(NAMESERVER_OBJ) $(CHUNKSERVER_OBJ) $(PROTO_OBJ) $(SDK_OBJ): $(PROTO_HEADER)
$(NAMESERVER_OBJ): $(NAMESERVER_HEADER)
$(CHUNKSERVER_OBJ): $(CHUNKSERVER_HEADER)
$(SDK_OBJ): $(SDK_HEADER)

# Targets
nameserver: $(NAMESERVER_OBJ) $(OBJS) $(LEVELDB)
	$(CXX) $(NAMESERVER_OBJ) $(OBJS) -o $@ $(LDFLAGS)

chunkserver: $(CHUNKSERVER_OBJ) $(OBJS) $(LEVELDB)
	$(CXX) $(CHUNKSERVER_OBJ) $(OBJS) -o $@ $(LDFLAGS)

libbfs.a: $(SDK_OBJ) $(OBJS) $(PROTO_HEADER)
	$(AR) -rs $@ $(SDK_OBJ) $(OBJS)

bfs_client: $(CLIENT_OBJ) $(LIBS) $(LEVELDB)
	$(CXX) $(CLIENT_OBJ) $(LIBS) -o $@ $(LDFLAGS)

$(LEVELDB):
	cd thirdparty/leveldb; make -j4

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(INCLUDE_PATH) -c $< -o $@

%.pb.h %.pb.cc: %.proto
	$(PROTOC) --proto_path=./src/proto/ --proto_path=/usr/local/include --cpp_out=./src/proto/ $<

clean:
	rm -rf $(BIN)
	rm -rf $(NAMESERVER_OBJ) $(CHUNKSERVER_OBJ) $(SDK_OBJ) $(CLIENT_OBJ) $(OBJS)
	rm -rf $(PROTO_SRC) $(PROTO_HEADER)

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

