#! /bin/bash

ns_num=1
if [ "$1"x = "raft"x ]; then
    ns_num=3;
fi

for((i=0;i<$ns_num;i++))
do
    cd nameserver$i;
    ./bin/nameserver --node_index=$i 1>nlog 2>&1 &
    echo $! > pid
    cd -
done;

for i in `seq 0 3`;
do
    cd chunkserver$i;
    ./bin/chunkserver --chunkserver_port=802$i 1>clog1 2>&1 &
    echo $! > pid
    cd -
done
