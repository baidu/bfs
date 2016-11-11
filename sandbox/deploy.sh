#!/usr/bin/env bash
set -x
set -o pipefail
export PS4='+{$LINENO `date "+%Y-%m-%d_%H:%M:%S"` :${FUNCNAME[0]}}    '
cur=`dirname "${0}"`
cd "${cur}"
cur=`pwd`

bash ./clear.sh

ns_num=2
strategy=$1

if [ "$1"x = "x" ]; then
    strategy="none";
    ns_num=1;
elif [ "$1x" == "raftx" ]; then
    ns_num=3
elif [ "$1x" == "master_slave" ]; then
    ns_num=2
fi

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
echo '--ha_strategy=$strategy' >> bfs.flag
echo '--nameserver_nodes=127.0.0.1:8827,127.0.0.1:8828,127.0.0.1:8829' >> bfs.flag

for((i=0;i<$ns_num;i++));
do
    mkdir -p nameserver$i/bin
    mkdir -p nameserver$i/log
    cp -f ../nameserver nameserver$i/bin/
    cp -f bfs.flag nameserver$i/
done

for i in `seq 0 3`;
do
    mkdir -p chunkserver$i/bin
    mkdir -p chunkserver$i/data1
    mkdir -p chunkserver$i/data2
    mkdir -p chunkserver$i/log
    cp -f ../chunkserver chunkserver$i/bin/
    cp bfs.flag chunkserver$i/
done

cp -f ../bfs_client ./


