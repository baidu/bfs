#! /bin/sh

./clear.sh

echo '--nameserver=127.0.0.1' > bfs.flag
echo '--default_replica_num=3' >> bfs.flag
echo '--chunkserver_log_level=2' >> bfs.flag
echo '--blockreport_interval=2' >> bfs.flag
echo '--nameserver_log_level=2' >> bfs.flag
echo '--keepalive_timeout=10' >> bfs.flag
echo '--nameserver_safemode_time=1' >> bfs.flag
echo '--block_store_path=./data1,./data2' >> bfs.flag
echo '--bfs_bug_tolerant=false' >> bfs.flag
echo '--raft_nodes=127.0.0.1:8827,127.0.0.1:8828,127.0.0.1:8829' >> bfs.flag
echo '--select_chunkserver_local_factor=0' >> bfs.flag
echo '--bfs_web_kick_enable=true' >> bfs.flag
echo '--ha_strategy=raft' >> bfs.flag

for i in `seq 0 2`;
do
    mkdir -p nameserver$i/bin
    cp -f ../nameserver nameserver$i/bin/
    cp -f bfs.flag nameserver$i/
done

echo '--nameserver_port=8828' >> bfs.flag
for i in `seq 0 3`;
do
    mkdir -p chunkserver$i/bin
    mkdir -p chunkserver$i/data1
    mkdir -p chunkserver$i/data2
    cp -f ../chunkserver chunkserver$i/bin/
    cp bfs.flag chunkserver$i/
done

cp -f ../bfs_client ./


