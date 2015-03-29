
# OPT ?= -O2 -DNDEBUG     # (A) Production use (optimized mode)
OPT ?= -g2 -Wall          # (B) Debug mode, w/ full line-level debugging symbols
# OPT ?= -O2 -g2 -DNDEBUG # (C) Profiling mode: opt, but w/debugging symbols

# Thirdparty
SNAPPY_PATH=./third_party/snappy/
PROTOBUF_PATH=./third_party/protobuf/
PROTOC_PATH=
PROTOC=$(PROTOC_PATH)protoc
PBRPC_PATH=./third_party/sofa-pbrpc/output/
BOOST_PATH=../boost/

INCLUDE_PATH = -I./src -I$(PROTOBUF_PATH)/include \
               -I$(PBRPC_PATH)/include \
               -I./third_party/leveldb/include \
               -I$(SNAPPY_PATH)/include \
               -I$(BOOST_PATH)/include

LDFLAGS = -L$(PROTOBUF_PATH)/lib -lprotobuf \
          -L$(PBRPC_PATH)/lib -lsofa-pbrpc \
          -L./third_party/leveldb -lleveldb \
          -L$(SNAPPY_PATH)/lib -lsnappy \
          -lpthread -lz

CXXFLAGS += $(OPT)

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

FLAGS_OBJ = $(patsubst %.cc, %.o, $(wildcard src/*.cc))
COMMON_OBJ = $(patsubst %.cc, %.o, $(wildcard src/common/*.cc))
OBJS = $(FLAGS_OBJ) $(COMMON_OBJ) $(PROTO_OBJ)

LIBS = libbfs.a
BIN = nameserver chunkserver bfs_client

all: $(BIN)

# Depends
$(NAMESERVER_OBJ) $(CHUNKSERVER_OBJ) $(PROTO_OBJ) $(SDK_OBJ): $(PROTO_HEADER)
$(NAMESERVER_OBJ): $(NAMESERVER_HEADER)
$(CHUNKSERVER_OBJ): $(CHUNKSERVER_HEADER)
$(SDK_OBJ): $(SDK_HEADER)

# Targets
nameserver: $(NAMESERVER_OBJ) $(OBJS)
	$(CXX) $(NAMESERVER_OBJ) $(OBJS) -o $@ $(LDFLAGS)

chunkserver: $(CHUNKSERVER_OBJ) $(OBJS)
	$(CXX) $(CHUNKSERVER_OBJ) $(OBJS) -o $@ $(LDFLAGS)

libbfs.a: $(SDK_OBJ) $(OBJS) $(PROTO_HEADER)
	$(AR) -rs $@ $(SDK_OBJ) $(OBJS)

bfs_client: $(CLIENT_OBJ) $(LIBS)
	$(CXX) $(CLIENT_OBJ) $(LIBS) -o $@ $(LDFLAGS)

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(INCLUDE_PATH) -c $< -o $@

%.pb.h %.pb.cc: %.proto
	$(PROTOC) --proto_path=./src/proto/ --proto_path=/usr/local/include --cpp_out=./src/proto/ $<

clean:
	rm -rf $(BIN)
	rm -rf $(NAMESERVER_OBJ) $(CHUNKSERVER_OBJ) $(SDK_OBJ) $(CLIENT_OBJ) $(OBJS)
	rm -rf $(PROTO_SRC) $(PROTO_HEADER)

.PHONY: test
test:
	echo "Test done"

