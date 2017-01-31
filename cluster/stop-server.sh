#!/usr/bin/env bash
#set -x -e

ns_num=0
cs_num=0
strategy="none"
declare -a ns
declare -a ns_addr
declare -a cs
declare -a cs_addr

#check servers format
if [ $# -lt 1 ]; then
    echo "no server list file get"
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
        echo "only support two nameserver in master_slave mode"
        exit
    fi
fi
#check chunkserver num
if [ $cs_num -lt 1 ]; then
    echo "need one chunkserver at least"
    exit
fi
#find the server to stop and stop
#stop ns
for((i=0;i<$ns_num;i++));
do
    servername=`echo ${ns[$i]} |awk -F ' ' '{print $2}'`
    ip=`echo ${ns[$i]} |awk -F ' ' '{print $3}'`
    username=`echo ${ns[$i]} |awk -F ' ' '{print $6}'`
    password=`echo ${ns[$i]} |awk -F ' ' '{print $7}'`
    bfslocation=`echo ${ns[$i]} |awk -F ' ' '{print $9}'`
    if [ $servername = $2 ]; then
        stop=${ns[$i]}
        sshpass -p $password ssh -o StrictHostKeyChecking=no $username@$ip "cd $bfslocation
        kill \`cat pid\`
        rm -f pid"
        exit
    fi
done

#stop cs
if [ ! -n "$stop" ]; then
    for((i=0;i<$cs_num;i++));
    do
        servername=`echo ${cs[$i]} |awk -F ' ' '{print $2}'`
        ip=`echo ${cs[$i]} |awk -F ' ' '{print $3}'`
        username=`echo ${cs[$i]} |awk -F ' ' '{print $6}'`
        password=`echo ${cs[$i]} |awk -F ' ' '{print $7}'`
        bfslocation=`echo ${cs[$i]} |awk -F ' ' '{print $9}'`
        if [ $servername = $2 ]; then
            stop=${cs[$i]}
            sshpass -p $password ssh -o StrictHostKeyChecking=no $username@$ip "cd $bfslocation
            kill \`cat pid\`
            rm -f pid"
            exit
        fi
    done
fi
