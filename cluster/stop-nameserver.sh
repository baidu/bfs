#!/usr/bin/env bash

#set -x -e

ns_num=0
cs_num=0
strategy="none"
declare -a ns
declare -a ns_addr
declare -a cs
declare -a cs_addr

#foreach servers file
while read line
do
    type=`echo $line |awk -F ' ' '{print $1}'`
    if [ "$type"x = "strategy"x ]; then
        strategy=`echo $line |awk -F ' ' '{print $2}'`
        if [ "$strategy"x != "master_slave"x ] && [ "$strategy"x != "none"x ]; then
            echo "don't support $strategy mode"
            exit
        fi
    elif [ $type = "server" ]; then
        ip=`echo $line |awk -F ' ' '{print $3}'`
        port=`echo $line |awk -F ' ' '{print $4}'`
        role=`echo $line |awk -F ' ' '{print $8}'`
        if [ "$role"x = "nameserver"x ]; then
            ns[$ns_num]=$line
            ns_addr[$ns_num]=$ip:$port
            ((ns_num=ns_num+1))
        elif [ "$role"x = "chunkserver"x ]; then
            cs[$cs_num]=$line
            cs_addr[$cs_num]=$ip:$port
            ((cs_num=cs_num+1))
        fi
    else
        echo "servers type $type error"
        exit
    fi
done < $1
#check nameserver num
if [ "$strategy"x = "none"x ]; then
    if [ $ns_num -ne 1 ]; then
        echo "only support one nameserver in none mode"
        exit
    fi
elif [ "$strategy"x == "master_slave" ]; then
    if [ $ns_num -ne 2 ]; then
        echo "only support one nameserver in master_slave mode"
        exit
    fi
fi
#check chunkserver num
if [ $cs_num -lt 1 ]; then
    echo "need one chunkserver at least"
    exit
fi
#start ns
if [ "$strategy"x == "none"x ]; then
    servername=`echo ${ns[$i]} |awk -F ' ' '{print $2}'`
    sh stop-server.sh $1 $servername
elif [ "$strategy"x == "master_slave"x ]; then
    servername=`echo ${ns[0]} |awk -F ' ' '{print $2}'`
    sh stop-server.sh $1 $servername

    servername=`echo ${ns[1]} |awk -F ' ' '{print $2}'`
    sh stop-server.sh $1 $servername
fi
