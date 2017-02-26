#!/usr/bin/env bash
source ./ini.sh

#check args format
if [ ! -f cluster.ini ]; then 
    echo "cluster.ini don't exist"
    exit
fi

#judge server exist or not
servers=`get cluster.ini server servers`
for server in $servers
do
    role=`get cluster.ini $server role`
    if [ "$role"x == "chunkserver"x ]; then
        sh start-server.sh $server
    fi
done
