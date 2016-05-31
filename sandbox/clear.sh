#! /bin/bash

killall -9 nameserver
killall -9 chunkserver
killall -9 bfs_client

rm -rf nameserver* chunkserver*
rm -rf master* slave*
rm -rf bfs_client
rm -rf bfs.flag
rm -rf client.*
rm -rf core.*
