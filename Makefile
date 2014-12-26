all: nameserver chunkserver

SNAPPY_PATH=./snappy/
PROTOBUF_PATH=./third_party/protobuf/
PROTOC_PATH=
PBRPC_PATH=./sofa-pbrpc/output/
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

PROTOC=$(PROTOC_PATH)protoc

.PHONY: proto test

proto: ./src/proto/file.proto ./src/proto/nameserver.proto src/proto/chunkserver.proto
	$(PROTOC) --proto_path=./src/proto/ --proto_path=/usr/local/include --cpp_out=./src/proto/ ./src/proto/*.proto

NAMESERVER_SRC = src/nameserver/nameserver_impl.cc src/nameserver/nameserver_main.cc \
				 src/proto/nameserver.pb.cc src/proto/file.pb.cc src/flags.cc
NAMESERVER_HEADER = src/nameserver/nameserver_impl.h src/proto/nameserver.pb.h src/proto/file.pb.h

nameserver: $(NAMESEVER_SRC) $(NAMESERVER_HEADER)
	$(CXX) $(NAMESERVER_SRC) $(INCLUDE_PATH) $(LDFLAGS) -o $@

CHUNKSERVER_SRC = src/chunkserver/chunkserver_impl.cc src/chunkserver/chunkserver_main.cc \
				  src/proto/chunkserver.pb.cc src/proto/nameserver.pb.cc src/proto/file.pb.cc src/flags.cc
CHUNKSERVER_HEADER = src/chunkserver/chunkserver_impl.h src/proto/chunkserver.pb.h src/proto/file.pb.h

chunkserver: $(CHUNKSERVER_SRC) $(CHUNKSERVER_HEADER)
	$(CXX) $(CHUNKSERVER_SRC) $(INCLUDE_PATH) $(LDFLAGS) -o $@
clean:
	rm -rf nameserver
	rm -rf chunkserver
	rm -rf src/proto/*.pb.h
	rm -rf src/proto/*.pb.cc

test:
	echo "Test done"
