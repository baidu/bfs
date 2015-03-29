#! /bin/sh

mkdir -p nameserver/bin
mkdir -p chunkserver1/bin
mkdir -p chunkserver1/data
mkdir -p chunkserver2/bin
mkdir -p chunkserver2/data

cp -f ../nameserver nameserver/bin/
cp -f ../chunkserver chunkserver1/bin/
cp -f ../chunkserver chunkserver2/bin/
cp -f ../bfs_client ./
