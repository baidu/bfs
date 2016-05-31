#! /bin/bash

ns_num=1
if [ "$1"x = "raft"x ]; then
    ns_num=3;
elif [ "$1"x == "ms"x ]; then
    ns_num=0;
fi

for((i=0;i<$ns_num;i++))
do
    cd nameserver$i;
    port=$((i+8827))
    ./bin/nameserver --raft_node_index=$i 1>nlog 2>&1 &
    echo $! > pid
    cd -
done;

if [ "$1"x == "ms"x ]; then
    cd nameserver0;
    ./bin/nameserver --master_slave_role=slave --node_index=0 1>nlog 2>&1 &
    echo $! > pid
    cd -

    cd nameserver1;
    ./bin/nameserver --master_slave_role=master --node_index=1 1>nlog 2>&1 &
    echo $! > pid
    cd -
fi

for i in `seq 0 3`;
do
    cd chunkserver$i;
    ./bin/chunkserver --chunkserver_port=802$i 1>clog1 2>&1 &
    echo $! > pid
    cd -
done
