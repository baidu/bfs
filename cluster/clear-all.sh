#!/bin/bash
sh stop-all.sh $1
ns_num=0
cs_num=0
strategy="none"
declare -a ns
declare -a ns_addr
declare -a cs
declare -a cs_addr

function print_usage()
{
    echo "$0 serverfile"
    echo "example"
    echo "$0 serverfile server_file"
}

#check servers format
if [ $# -ne 1 ]; then
    usage
    exit
fi

if [ ! -f $1 ]; then 
    echo "$1 don't exist"
    exit
fi
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
    elif [ "$type"x = "server"x ]; then
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
#foreach servers file
#delete ns
for((i=0;i<$ns_num;i++));
do
    ip=`echo ${ns[$i]} |awk -F ' ' '{print $3}'`
    hostname=`echo ${ns[$i]} |awk -F ' ' '{print $5}'`
    username=`echo ${ns[$i]} |awk -F ' ' '{print $6}'`
    password=`echo ${ns[$i]} |awk -F ' ' '{print $7}'`
    bfslocation=`echo ${ns[$i]} |awk -F ' ' '{print $9}'`
    sshpass -p $password ssh -o StrictHostKeyChecking=no $username@$ip "rm -rf $bfslocation"
done
#delete cs
for((i=0;i<$cs_num;i++));
do
    ip=`echo ${cs[$i]} |awk -F ' ' '{print $3}'`
    username=`echo ${cs[$i]} |awk -F ' ' '{print $6}'`
    password=`echo ${cs[$i]} |awk -F ' ' '{print $7}'`
    bfslocation=`echo ${cs[$i]} |awk -F ' ' '{print $9}'`
    sshpass -p $password ssh -o StrictHostKeyChecking=no $username@$ip "rm -rf $bfslocation"
done
