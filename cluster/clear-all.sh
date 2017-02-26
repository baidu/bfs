#!/bin/bash
#set -x -e
source ./ini.sh
sh stop-all.sh

#judge server exist or not
servers=`get cluster.ini server servers`
for server in $servers
do
    #get server info
    ip=`get cluster.ini $server ip`
    username=`get cluster.ini $server username`
    serverpath=`get cluster.ini $server serverpath`
    ssh $username@$ip "rm -rf $serverpath"
done
