#!/usr/bin/env bash
function print_usage()
{
    echo "$0 servername"
    echo "example"
}

source ./ini.sh

#check args format
if [ $# -lt 1 ]; then
    print_usage
    exit
fi

if [ ! -f cluster.ini ]; then 
    echo "cluster.ini don't exist"
    exit
fi

#judge server exist or not
servers=`get cluster.ini server servers`
check=`echo $servers | grep $1`
if [ "$check"x = ""x ]; then
    echo not in
fi
#get server info
ip=`get cluster.ini $1 ip`
port=`get cluster.ini $1 port`
role=`get cluster.ini $1 role`
username=`get cluster.ini $1 username`
serverpath=`get cluster.ini $1 serverpath`
index=`get cluster.ini $1 index`
strategy=`get cluster.ini strategy strategy`
#start server
if [ "$role"x == "nameserver"x ]; then
    if [ "$strategy"x == "none"x ] || [ "$strategy"x == ""x ]; then 
        #start nameserver
        ssh $username@$ip "cd $serverpath
        nohup ./bin/nameserver --node_index=0 1>nlog 2>&1 &
        echo \$! > pid"
    elif [ "$strategy"x == "master_slave"x ]; then
        #start chunkserver
        ssh $username@$ip "cd $serverpath
        nohup ./bin/nameserver --node_index=$index 1>nlog 2>&1 &
        echo \$! > pid"
    else
        echo "can not support $strategy strategy"
    fi
elif [ "$role"x == "chunkserver"x ]; then
    #start
    ssh $username@$ip "cd $serverpath
    nohup ./bin/chunkserver --chunkserver_port=$port 1>nlog 2>&1 &
    echo \$! > pid"
else
    echo "config error: can not support role$role"
fi
