#!/usr/bin/env bash

ns_num=-1
strategy=$1
ns_roles=("" "" "")

if [ "$strategy"x == "x" ]; then
    strategy="none";
    ns_num=1;
elif [ "$strategy"x == "raftx" ]; then
    ns_num=3
elif [ "$strategy"x == "master_slavex" ]; then
    ns_num=2
    ns_roles[0]="--master_slave_role=slave"
    ns_roles[1]="--master_slave_role=master"
else
    echo -e "Usage:\t./deplay.sh [raft|master_slave]"
    exit -1
fi
echo -e "nameserver_cnt=$ns_num\tstrategy=$strategy"

bash ./clear.sh

echo '--default_replica_num=3' >> bfs.flag
echo '--chunkserver_log_level=2' >> bfs.flag
echo '--blockreport_interval=2' >> bfs.flag
echo '--bfs_log=./log/bfs.log' >> bfs.flag
echo '--nameserver_log_level=2' >> bfs.flag
echo '--keepalive_timeout=10' >> bfs.flag
echo '--nameserver_start_recover_timeout=15' >> bfs.flag
echo '--block_store_path=./data1,./data2' >> bfs.flag
echo '--bfs_bug_tolerant=false' >> bfs.flag
echo '--select_chunkserver_local_factor=0' >> bfs.flag
echo '--bfs_web_kick_enable=true' >> bfs.flag
echo "--ha_strategy=$strategy" >> bfs.flag
echo '--nameserver_nodes=127.0.0.1:8827,127.0.0.1:8828,127.0.0.1:8829' >> bfs.flag
echo '--sdk_wirte_mode=fanout' >> bfs.flag
echo '--chunkserver_multi_path_on_one_disk=true' >> bfs.flag


for((i=0;i<$ns_num;i++));
do
    mkdir -p nameserver$i/bin
    mkdir -p nameserver$i/log
    cp -f ../nameserver nameserver$i/bin/
    cp -f bfs.flag nameserver$i/
    cd nameserver$i;
    ./bin/nameserver ${ns_roles[$i]} --node_index=$i 1>nlog 2>&1 &
    echo $! > pid
    cd -
done

for i in `seq 0 3`;
do
    mkdir -p chunkserver$i/bin
    mkdir -p chunkserver$i/data1
    mkdir -p chunkserver$i/data2
    mkdir -p chunkserver$i/log
    cp -f ../chunkserver chunkserver$i/bin/
    cp bfs.flag chunkserver$i/
    
    cd chunkserver$i;
    ./bin/chunkserver --chunkserver_port=802$i 1>clog1 2>&1 &
    echo $! > pid
    cd -
done

cp -f ../bfs_client ./


