#! /bin/sh

for i in `seq 0 2`;
do
    cd nameserver$i;
    ./bin/nameserver --nameserver_port=882$i 1>nlog1 2>&1 &
    echo $! > pid
    cd ../
done

for i in `seq 0 3`;
do
    cd chunkserver$i;
    ./bin/chunkserver --chunkserver_port=802$i 1>clog1 2>&1 &
    echo $! > pid
    cd ..
done
