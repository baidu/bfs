
ROOT_DIR=../../../../
PROTOBUF_DIR=$(ROOT_DIR)/third-64/protobuf/lib
SOFARPC_DIR=$(ROOT_DIR)/public/sofa-pbrpc/lib
SNAPPY_DIR=$(ROOT_DIR)/third-64/snappy/lib
all: sync_test

sync_test: sync_test.cc
	g++ -o sync_test sync_test.cc -I../output/include/ -L../output/lib -lbfs -lz -lpthread\
		-L$(PROTOBUF_DIR) -lprotobuf \
		-L$(SOFARPC_DIR) -lsofa_pbrpc \
		-L$(SNAPPY_DIR) -lsnappy

clean:
		rm -rf sync_test
