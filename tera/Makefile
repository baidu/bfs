
OPT ?= -g2 -Wall

INCLUDE_PATH= -I../output/include

CXXFLAGS += $(OPT)

LDFLAGS=-L../output/lib -lbfs

all: bfs_wrapper.so

bfs_wrapper.so: bfs_wrapper.cc bfs_wrapper.h
	g++ -g -Wall -shared -fPIC $(INCLUDE_PATH) $(LDFLAGS) -o $@ bfs_wrapper.cc \
		-Xlinker "-(" ../libbfs.a \
		-Xlinker "-)"
	

clean:
	rm -rf bfs_wrapper.so
