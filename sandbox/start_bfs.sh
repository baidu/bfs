#! /bin/sh

cd nameserver;
./bin/nameserver 1>nlog1 &2>1 &

cd ../chunkserver1;
./bin/chunkserver --chunkserver_port=8021 1>clog1 2>&1 &

cd ../chunkserver2;
./bin/chunkserver --chunkserver_port=8022 1>clog2 2>&1 &

