#!/usr/bin/env bash
set -x
set -o pipefail
export PS4='+{$LINENO `date "+%Y-%m-%d_%H:%M:%S"` :${FUNCNAME[0]}}    '
cur=`dirname "${0}"`
cd "${cur}"
cur=`pwd`

NOSE_PATH=../thirdsrc/

if [ ! -d ./nose ]
then
    cp ${NOSE_PATH}/nose.tar.gz ./
    tar -xzvf nose.tar.gz
fi

cp ../mark ./
cp ../sandbox/*.sh ./
mkdir -p ./log

sh -x prepare_data.sh &>./log/prepare_data.log

# test start and stop nameserver
nosetests test_nameserver_start_stop.py -s -v -x &>./log/test_nameserver_start_stop.log 
if [ $? -ne 0 ]
then
    echo "test_nameserver_start_stop.py case FAIL, exit"
    exit 1 
fi

# use mark test read and write
nosetests test_put_read.py -s -v -x &>./log/test_put_read.log
if [ $? -ne 0 ]
then
    echo "test_put_read.py case FAIL, exit"
    exit 1
fi

# use bfs_client test file operate
nosetests test_file_operate.py -s -v -x &>./log/test_file_operate.log
if [ $? -ne 0 ]
then
    echo "test_file_operate.py case FAIL, exit"
    exit 1
fi

exit 0

