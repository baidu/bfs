#! /bin/sh

echo '--nameserver=127.0.0.1' > bfs.flag
echo '--default_replica_num=3' >> bfs.flag
echo '--chunkserver_log_level=2' >> bfs.flag
echo '--nameserver_log_level=2' >> bfs.flag
echo '--block_store_path=./data1,./data2' >> bfs.flag
echo '--cluster_members=127.0.0.1:8868,127.0.0.1:8869,127.0.0.1:8870,127.0.0.1:8871,127.0.0.1:8872' >> bfs.flag

for i in `seq 0 2`;
do
    mkdir -p nameserver$i/bin
    cp bfs.flag nameserver$i/
    cp -f ../nameserver nameserver$i/bin/
done

for i in `seq 0 3`;
do
    mkdir -p chunkserver$i/bin
    mkdir -p chunkserver$i/data1
    mkdir -p chunkserver$i/data2
    cp -f ../chunkserver chunkserver$i/bin/
    cp bfs.flag chunkserver$i/
done

cp -f ../bfs_client ./

