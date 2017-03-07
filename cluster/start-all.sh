#!/usr/bin/env bash
source ./ini.sh
#start ns
echo "starting nameserver..."
sh start-nameserver.sh
#start cs
echo "starting chunkserver..."
sh start-chunkserver.sh
