#! /bin/sh

cd nameserver;
./bin/nameserver 1>nlog1 2>&1 &
echo $! > pid

for ((i=0;i<4;i++)) do
    cd ../chunkserver$i;
    ./bin/chunkserver --chunkserver_port=802$i 1>clog1 2>&1 &
    echo $! > pid
done
