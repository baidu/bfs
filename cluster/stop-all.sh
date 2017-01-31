#!/usr/bin/env bash

#stop ns
echo "stoping nameserver..."
sh stop-nameserver.sh $1
#start cs
echo "stoping chunkserver..."
sh stop-chunkserver.sh $1
