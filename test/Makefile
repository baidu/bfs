

PBRPC_PATH=../thirdparty/sofa-pbrpc/output/

all: sync_test
sync_test: sync_test.cc
	g++ -o sync_test sync_test.cc -I../output/include \
		-L../output/lib -lbfs \
		-L$(PBRPC_PATH)/lib -lsofa-pbrpc \
		-lprotobuf -lsnappy -lz -lgflags
clean:
		rm -rf sync_test
