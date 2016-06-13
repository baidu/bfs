#!/bin/bash

NOSE_PATH=../thirdsrc/

if [ -d ./nose ]
then
    cp ${NOSE_PATH}/nose.tar.gz ./
    tar -xzvf nose.tar.gz
fi

cp ../mark ./
cp ../sandbox/*.sh ./

STATUS=0

# test start and stop nameserver
nosetests test_nameserver_start_stop.py -s
if [ $? -ne 0 ]
then
    echo "test_nameserver_start_stop.py case FAIL, exit"
    STATUS=1
fi

# use mark test read and write
nosetests test_put_read.py -s
if [ $? -ne 0 ]
then
    echo "test_put_read.py case FAIL, exit"
    STATUS=1
fi

# use bfs_client test file operate
nosetests test_file_operate.py -s
if [ $? -ne 0 ]
then
    echo "test_file_operate.py case FAIL, exit"
    STATUS=1
fi

exit ${STATUS}
