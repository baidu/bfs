#!/usr/bin/env bash
source ./ini.sh
#stop ns
echo "stoping nameserver..."
sh stop-nameserver.sh
#start cs
echo "stoping chunkserver..."
sh stop-chunkserver.sh
