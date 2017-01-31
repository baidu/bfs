#!/usr/bin/env bash

#start ns
echo "starting nameserver..."
sh start-nameserver.sh $1
#start cs
echo "starting chunkserver..."
sh start-chunkserver.sh $1
