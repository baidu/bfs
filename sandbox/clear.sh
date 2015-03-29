#! /bin/sh

killall -9 nameserver
killall -9 chunkserver
killall -9 bfs_client

rm -rf nameserver chunkserver*
rm -rf bfs_client
