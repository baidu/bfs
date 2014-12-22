all: proto nameserver chunkserver

INCLUDE_PATH = -I./src -I./third_party/protobuf/include \
			   -I./third_party/sofa-pbrpc/include \
			   -I./third_party/leveldb/include \
			   -I./third_party/boost/include
LDFLAGS = -L./third_party/protobuf/lib -lprotobuf \
		  -L./third_party/sofa-pbrpc/lib -lsofa-pbrpc \
		  -L./third_party/leveldb -lleveldb \
		  -L./third_party/snappy/lib -lsnappy \
		  -lpthread -lz
PROTOC=./third_party/protobuf/bin/protoc

proto: ./src/proto/file.proto ./src/proto/nameserver.proto src/proto/chunkserver.proto
	$(PROTOC) --proto_path=./src/proto/ --cpp_out=./src/proto/ ./src/proto/*.proto

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

.PHONY: test
test:
	echo "Test done"
