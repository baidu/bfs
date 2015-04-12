INCLUDE_PATH = ../

CXXFLAGS= -I$(INCLUDE_PATH)
LDFLAGS= -lpthread

TEST_SRC= $(wildcard *_test.cc)
TEST_HEADER= $(patsubst %_test.cc,%.h,$(TEST_SRC))
TEST_TARGE= $(patsubst %.cc,%,$(TEST_SRC))

all: $(TEST_TARGE)

clean:
	rm -rf $(TEST_TARGE)

