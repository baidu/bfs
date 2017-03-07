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
#stop server
ssh $username@$ip "cd $serverpath
kill \`cat pid\`
rm -f pid"
