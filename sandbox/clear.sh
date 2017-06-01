#!/usr/bin/env bash

killall -9 nameserver  > /dev/null 2>&1
killall -9 chunkserver > /dev/null 2>&1
killall -9 bfs_client  > /dev/null 2>&1

rm -rf nameserver* chunkserver*
rm -rf master* slave*
rm -rf bfs_client
rm -rf bfs.flag
rm -rf client.*
rm -rf core.*
