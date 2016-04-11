#! /bin/sh

./clear.sh

mkdir -p nameserver/bin

echo '--nameserver=127.0.0.1' > bfs.flag
echo '--nameserver_port=8828' >> bfs.flag
echo '--default_replica_num=3' >> bfs.flag
echo '--chunkserver_log_level=2' >> bfs.flag
echo '--blockreport_interval=2' >> bfs.flag
echo '--nameserver_log_level=2' >> bfs.flag
echo '--keepalive_timeout=10' >> bfs.flag
echo '--nameserver_safemode_time=1' >> bfs.flag
device=`df . | tail -n 1 | awk '{print $1}'`
echo "--block_store_path=./data1:$device,./data2:$device" >> bfs.flag
echo '--bfs_bug_tolerant=false' >> bfs.flag
echo '--select_chunkserver_local_factor=0' >> bfs.flag

cp bfs.flag nameserver/

for i in `seq 0 3`;
do
    mkdir -p chunkserver$i/bin
    mkdir -p chunkserver$i/data1
    mkdir -p chunkserver$i/data2
    cp -f ../chunkserver chunkserver$i/bin/
    cp bfs.flag chunkserver$i/
done

cp -f ../nameserver nameserver/bin/
cp -f ../bfs_client ./


