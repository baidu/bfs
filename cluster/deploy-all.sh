#!/bin/bash
source ./ini.sh

#create bfs.flag
strategy=`get cluster.ini strategy strategy`
servers=`get cluster.ini server servers`
echo "creating bfs.flag..."
echo '--default_replica_num=3' > bfs.flag
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
echo -n '--nameserver_nodes=' >> bfs.flag
for server in $servers
do
    ip=`get cluster.ini $server ip`
    port=`get cluster.ini $server port`
    role=`get cluster.ini $server role`
    if [ "$role"x = "nameserver"x ]; then
        echo -n "$ip:$port," >> bfs.flag
    fi
done
echo "" >> bfs.flag
echo '--sdk_wirte_mode=fanout' >> bfs.flag
echo '--chunkserver_multi_path_on_one_disk=true' >> bfs.flag
cp -f ../bfs_client ./
#send dir to destinetion dir
for server in $servers
do
    ip=`get cluster.ini $server ip`
    port=`get cluster.ini $server port`
    role=`get cluster.ini $server role`
    username=`get cluster.ini $server username`
    serverpath=`get cluster.ini $server serverpath`
    strategy=`get cluster.ini strategy strategy`
    if [ "$role"x == "nameserver"x ]; then
        mkdir -p $server/bin $server/log
        cp -f ../nameserver $server/bin/
        cp -f bfs.flag $server/
        scp -r $server $username@$ip:$serverpath
        rm -rf $server
    elif [ "$role"x == "chunkserver"x ]; then
        mkdir -p $server/bin
        mkdir -p $server/data1
        mkdir -p $server/data2
        mkdir -p $server/log
        cp -f ../chunkserver $server/bin/
        cp bfs.flag $server/
        scp -r $server $username@$ip:$serverpath
        rm -rf $server
    fi
done
echo "deploy succeed"
