#!/usr/bin/env bash

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
    echo "$0 serverfile servername"
}

#check servers format
if [ $# -lt 2 ]; then
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
elif [ "$strategy"x == "master_slave"x ]; then
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
#find the server to start and start
#find server
for((i=0;i<$ns_num;i++));
do
    servername=`echo ${ns[$i]} |awk -F ' ' '{print $2}'`
    if [ $servername = $2 ]; then
        start=${ns[$i]}
        index=$i
    fi
done

if [ ! -n "$start" ]; then
    for((i=0;i<$cs_num;i++));
    do
        servername=`echo ${cs[$i]} |awk -F ' ' '{print $2}'`
        if [ $servername = $2 ]; then
            start=${cs[$i]}
        fi
    done
fi

#start $start
role=`echo $start |awk -F ' ' '{print $8}'`
#start ns
if [ "$role"x == "nameserver"x ]; then
    if [ "$strategy"x == "none"x ]; then
        ip=`echo $start |awk -F ' ' '{print $3}'`
        username=`echo $start |awk -F ' ' '{print $6}'`
        password=`echo $start |awk -F ' ' '{print $7}'`
        bfslocation=`echo $start |awk -F ' ' '{print $9}'`
        sshpass -p $password ssh -o StrictHostKeyChecking=no $username@$ip "cd $bfslocation
        nohup ./bin/nameserver --node_index=$index 1>nlog 2>&1 &
        echo \$! > pid"
    elif [ "$strategy"x == "master_slave"x ]; then
        ip=`echo $start |awk -F ' ' '{print $3}'`
        username=`echo $start |awk -F ' ' '{print $6}'`
        password=`echo $start |awk -F ' ' '{print $7}'`
        bfslocation=`echo $start |awk -F ' ' '{print $9}'`
        ms=`echo $start |awk -F ' ' '{print $10}'`
        if [ "$ms"x == "master"x ]; then
            index=0
        elif [ "$ms"x == "slave"x ]; then
            index=1
        fi
        sshpass -p $password ssh -o StrictHostKeyChecking=no $username@$ip "cd $bfslocation
        nohup ./bin/nameserver --master_slave_role=$ms --node_index=$index 1>nlog 2>&1 &
        echo \$! > pid"
    fi
elif [ "$role"x == "chunkserver"x ]; then
    #start cs
    ip=`echo $start |awk -F ' ' '{print $3}'`
    port=`echo $start |awk -F ' ' '{print $4}'`
    username=`echo $start |awk -F ' ' '{print $6}'`
    password=`echo $start |awk -F ' ' '{print $7}'`
    bfslocation=`echo $start |awk -F ' ' '{print $9}'`
    sshpass -p $password ssh -o StrictHostKeyChecking=no $username@$ip "cd $bfslocation
    nohup ./bin/chunkserver --chunkserver_port=$port 1>nlog 2>&1 &
    echo \$! > pid"
fi

