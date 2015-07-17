#! /bin/sh

mkdir -p nameserver/bin

echo '--nameserver=127.0.0.1' > bfs.flag
echo '--nameserver_port=8828' >> bfs.flag
echo '--default_replica_num=3' >> bfs.flag
echo '--chunkserver_log_level=2' >> bfs.flag
echo '--nameserver_log_level=2' >> bfs.flag
echo '--block_store_path=./data1,./data2' >> bfs.flag

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


