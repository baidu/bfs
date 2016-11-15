#!/usr/bin/env bash
set -x
set -o pipefail
export PS4='+{$LINENO `date "+%Y-%m-%d_%H:%M:%S"` :${FUNCNAME[0]}}    '
cur=`dirname "${0}"`
cd "${cur}"
cur=`pwd`

set -e -u -E # this script will exit if any sub-command fails

########################################
# download & build depend software
########################################

WORK_DIR=`pwd`
DEPS_SOURCE=`pwd`/thirdsrc
DEPS_PREFIX=`pwd`/thirdparty
DEPS_CONFIG="--prefix=${DEPS_PREFIX} --disable-shared --with-pic"
FLAG_DIR=`pwd`/.build

export PATH=${DEPS_PREFIX}/bin:$PATH
mkdir -p ${DEPS_SOURCE} ${DEPS_PREFIX} ${FLAG_DIR}

if [ ! -f "${FLAG_DIR}/dl_third" ] || [ ! -d "${DEPS_SOURCE}/.git" ]; then
    rm -rf ${DEPS_SOURCE}
    mkdir ${DEPS_SOURCE}
    git clone https://github.com/yvxiang/thirdparty.git thirdsrc
    touch "${FLAG_DIR}/dl_third"
fi

cd ${DEPS_SOURCE}

# boost
if [ ! -f "${FLAG_DIR}/boost_1_57_0" ] \
    || [ ! -d "${DEPS_PREFIX}/boost_1_57_0/boost" ]; then
    wget https://raw.githubusercontent.com/lylei9/boost_1_57_0/master/boost_1_57_0.tar.gz
    tar zxf boost_1_57_0.tar.gz
    rm -rf ${DEPS_PREFIX}/boost_1_57_0
    mv boost_1_57_0 ${DEPS_PREFIX}/boost_1_57_0
    touch "${FLAG_DIR}/boost_1_57_0"
fi

# protobuf
if [ ! -f "${FLAG_DIR}/protobuf_2_6_1" ] \
    || [ ! -f "${DEPS_PREFIX}/lib/libprotobuf.a" ] \
    || [ ! -d "${DEPS_PREFIX}/include/google/protobuf" ]; then
    cd protobuf-2.6.1
    autoreconf -ivf
    ./configure ${DEPS_CONFIG}
    make -j4
    make install
    cd -
    touch "${FLAG_DIR}/protobuf_2_6_1"
fi

#leveldb
if [ ! -f "${FLAG_DIR}/leveldb" ] \
    || [ ! -f "${DEPS_PREFIX}/lib/libleveldb.a" ] \
    || [ ! -d "${DEPS_PREFIX}/include/leveldb" ]; then
    rm -rf leveldb
    git clone https://github.com/lylei/leveldb.git leveldb
    cd leveldb
    echo "PREFIX=${DEPS_PREFIX}" > config.mk
    make -j4
    make install
    cd -
    touch "${FLAG_DIR}/leveldb"
fi

# snappy
if [ ! -f "${FLAG_DIR}/snappy_1_1_1" ] \
    || [ ! -f "${DEPS_PREFIX}/lib/libsnappy.a" ] \
    || [ ! -f "${DEPS_PREFIX}/include/snappy.h" ]; then
    cd snappy-1.1.1
    autoreconf -ivf
    ./configure ${DEPS_CONFIG}
    make -j4
    make install
    cd -
    touch "${FLAG_DIR}/snappy_1_1_1"
fi

# sofa-pbrpc
if [ ! -f "${FLAG_DIR}/sofa-pbrpc" ] \
    || [ ! -f "${DEPS_PREFIX}/lib/libsofa-pbrpc.a" ] \
    || [ ! -d "${DEPS_PREFIX}/include/sofa/pbrpc" ]; then
    rm -rf sofa-pbrpc

    git clone --depth=1 https://github.com/baidu/sofa-pbrpc.git sofa-pbrpc
    cd sofa-pbrpc
    sed -i '/BOOST_HEADER_DIR=/ d' depends.mk
    sed -i '/PROTOBUF_DIR=/ d' depends.mk
    sed -i '/SNAPPY_DIR=/ d' depends.mk
    echo "BOOST_HEADER_DIR=${DEPS_PREFIX}/boost_1_57_0" >> depends.mk
    echo "PROTOBUF_DIR=${DEPS_PREFIX}" >> depends.mk
    echo "SNAPPY_DIR=${DEPS_PREFIX}" >> depends.mk
    echo "PREFIX=${DEPS_PREFIX}" >> depends.mk
    make -j4
    make install
    cd -
    touch "${FLAG_DIR}/sofa-pbrpc"
fi

# cmake for gflags
if ! which cmake ; then
    cd CMake-3.2.1
    ./configure --prefix=${DEPS_PREFIX}
    make -j4
    make install
    cd -
fi

# gflags
if [ ! -f "${FLAG_DIR}/gflags_2_1_1" ] \
    || [ ! -f "${DEPS_PREFIX}/lib/libgflags.a" ] \
    || [ ! -d "${DEPS_PREFIX}/include/gflags" ]; then
    cd gflags-2.1.1
    cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DGFLAGS_NAMESPACE=google -DCMAKE_CXX_FLAGS=-fPIC
    make -j4
    make install
    cd -
    touch "${FLAG_DIR}/gflags_2_1_1"
fi

# gtest
if [ ! -f "${FLAG_DIR}/gtest_1_7_0" ] \
    || [ ! -f "${DEPS_PREFIX}/lib/libgtest.a" ] \
    || [ ! -d "${DEPS_PREFIX}/include/gtest" ]; then
    cd gtest-1.7.0
    ./configure ${DEPS_CONFIG}
    make
    cp -a lib/.libs/* ${DEPS_PREFIX}/lib
    cp -a include/gtest ${DEPS_PREFIX}/include
    cd -
    touch "${FLAG_DIR}/gtest_1_7_0"
fi

# libunwind for gperftools
if [ ! -f "${FLAG_DIR}/libunwind_0_99" ] \
    || [ ! -f "${DEPS_PREFIX}/lib/libunwind.a" ] \
    || [ ! -f "${DEPS_PREFIX}/include/libunwind.h" ]; then
    cd libunwind-0.99
    ./configure ${DEPS_CONFIG}
    make CFLAGS=-fPIC -j4
    make CFLAGS=-fPIC install
    cd -
    touch "${FLAG_DIR}/libunwind_0_99"
fi

# gperftools (tcmalloc)
if [ ! -f "${FLAG_DIR}/gperftools_2_2_1" ] \
    || [ ! -f "${DEPS_PREFIX}/lib/libtcmalloc_minimal.a" ]; then
    cd gperftools-2.2.1
    ./configure ${DEPS_CONFIG} CPPFLAGS=-I${DEPS_PREFIX}/include LDFLAGS=-L${DEPS_PREFIX}/lib
    make -j4
    make install
    cd -
    touch "${FLAG_DIR}/gperftools_2_2_1"
fi

# common
if [ ! -f "${FLAG_DIR}/common" ] \
    || [ ! -f "${DEPS_PREFIX}/lib/libcommon.a" ]; then
    rm -rf common
    git clone -b cpp11 https://github.com/baidu/common
    cd common
    sed -i 's/^PREFIX=.*/PREFIX=..\/..\/thirdparty/' config.mk
    sed -i '/^INCLUDE_PATH=*/s/$/ -I..\/..\/thirdparty\/boost_1_57_0/g' Makefile
    make -j4
    make install
    cd -
    touch "${FLAG_DIR}/common"
fi


cd ${WORK_DIR}

########################################
# config depengs.mk
########################################

echo "PBRPC_PATH=./thirdparty" > depends.mk
echo "PROTOBUF_PATH=./thirdparty" >> depends.mk
echo "PROTOC_PATH=./thirdparty/bin/" >> depends.mk
echo 'PROTOC=$(PROTOC_PATH)protoc' >> depends.mk
echo "PBRPC_PATH=./thirdparty" >> depends.mk
echo "BOOST_PATH=./thirdparty/boost_1_57_0" >> depends.mk
echo "GFLAG_PATH=./thirdparty" >> depends.mk
echo "GTEST_PATH=./thirdparty" >> depends.mk
echo "COMMON_PATH=./thirdparty" >> depends.mk
echo "TCMALLOC_PATH=./thirdparty" >> depends.mk

########################################
# build tera
########################################

make clean
make -j4

