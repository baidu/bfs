#!/bin/bash
#set -x -e

ns_num=0
cs_num=0
strategy="none"
declare -a ns
declare -a ns_addr
declare -a cs
declare -a cs_addr

function usage()
{
    echo "$0 serverfile"
}

#check servers format
if [ $# -lt 1 ]; then
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
        echo "only support two nameserver in master_slave mode"
        exit
    fi
fi
#check chunkserver num
if [ $cs_num -lt 1 ]; then
    echo "need one chunkserver at least"
    exit
fi

#create bfs.flag
echo "creating bfs.flag..."
echo '--default_replica_num=3' > bfs.flag
echo '--chunkserver_log_level=2' >> bfs.flag
echo '--blockreport_interval=2' >> bfs.flag
echo '--bfs_log=./log/bfs.log' >> bfs.flag
echo '--nameserver_log_level=2' >> bfs.flag
echo '--keepalive_timeout=10' >> bfs.flag
echo '--nameserver_start_recover_timeout=15' >> bfs.flag
echo '--block_store_path=./data1,./data2' >> bfs.flag
echo '--bfs_bug_tolerant=false' >> bfs.flag
echo '--select_chunkserver_local_factor=0' >> bfs.flag
echo '--bfs_web_kick_enable=true' >> bfs.flag
echo "--ha_strategy=$strategy" >> bfs.flag
echo -n '--nameserver_nodes=' >> bfs.flag
for ((i=0;i<$ns_num;i++));
do
    echo -n ${ns_addr[$i]}"," >> bfs.flag
done
echo "" >> bfs.flag
echo '--sdk_wirte_mode=fanout' >> bfs.flag
echo '--chunkserver_multi_path_on_one_disk=true' >> bfs.flag
#create dir
cp -f ../bfs_client ./
echo "creating nameserver..."
for((i=0;i<$ns_num;i++));
do
    mkdir -p nameserver$i/bin
    mkdir -p nameserver$i/log
    cp -f ../nameserver nameserver$i/bin/
    cp -f bfs.flag nameserver$i/
done
echo "creating chunkserver..."
for((i=0;i<$cs_num;i++));
do
    mkdir -p chunkserver$i/bin
    mkdir -p chunkserver$i/data1
    mkdir -p chunkserver$i/data2
    mkdir -p chunkserver$i/log
    cp -f ../chunkserver chunkserver$i/bin/
    cp bfs.flag chunkserver$i/
done
#send dir to destinetion dir
#send ns
for((i=0;i<$ns_num;i++));
do
    servername=`echo ${ns[$i]} |awk -F ' ' '{print $2}'`
    ip=`echo ${ns[$i]} |awk -F ' ' '{print $3}'`
    username=`echo ${ns[$i]} |awk -F ' ' '{print $6}'`
    password=`echo ${ns[$i]} |awk -F ' ' '{print $7}'`
    bfslocation=`echo ${ns[$i]} |awk -F ' ' '{print $9}'`
    echo "deploying $servername ..."
    sshpass -p $password scp -r nameserver$i $username@$ip:$bfslocation
done

#send cs
for((i=0;i<$cs_num;i++));
do
    servername=`echo ${cs[$i]} |awk -F ' ' '{print $2}'`
    ip=`echo ${cs[$i]} |awk -F ' ' '{print $3}'`
    port=`echo ${cs[$i]} |awk -F ' ' '{print $4}'`
    username=`echo ${cs[$i]} |awk -F ' ' '{print $6}'`
    password=`echo ${cs[$i]} |awk -F ' ' '{print $7}'`
    bfslocation=`echo ${cs[$i]} |awk -F ' ' '{print $9}'`
    echo "deploying $servername ..."
    sshpass -p $password scp -r chunkserver$i $username@$ip:$bfslocation
done
echo "deploy succeed"
#delete dir
for((i=0;i<$ns_num;i++));
do
    rm -rf nameserver$i
done
for((i=0;i<$cs_num;i++));
do
    rm -rf chunkserver$i
done

