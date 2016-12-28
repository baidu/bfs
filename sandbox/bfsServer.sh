#!/bin/sh

KILL=kill
RM=rm

declare -A bfs_status

function get_bfs_status
{
    #check nameserver
    for((index=0;;index++))
    do
        if [ -x nameserver$index ]; then
            if [ ! -f nameserver$index/pid ]; then
                bfs_status["nameserver$index"] = "nopid"
                if test $1 ; then
                    echo nameserver$index" get status failed (could not find file "nameserver$index/pid")"
                fi
            else
                pid=$(cat nameserver$index/pid)
                if [ $pid -eq 0 ]; then
                    continue
                fi
                stat=$(ps -p $pid -o stat=)
                if [ "${stat:0:1}"x = "D"x ]; then
                    bfs_status["nameserver$index"]="D"
                    if test $1 ; then
                        echo nameserver$index" is Dead"
                    fi
                elif [ "${stat:0:1}" = "Z" ]; then
                    bfs_status["nameserver$index"]="Z"
                    if test $1 ; then
                        echo nameserver$index" is Zomble"
                    fi
                else
                    bfs_status["nameserver$index"]="R"
                    if test $1 ; then
                        echo nameserver$index" is Running"
                    fi
                fi
            fi
        else
            break
        fi
    done

    #check chunkserver
    for((index=0;;index++))
    do
        if [ -x chunkserver$index ]; then
            if [ ! -f chunkserver$index/pid ]; then
                bfs_status["chunkserver$index"] = "nopid"
                if test $1 ; then
                    echo chunkserver$index" stop failed (could not find file "chunkserver$index/pid")"
                fi
            else
                pid=$(cat chunkserver$index/pid)
                if [ $pid -eq 0 ]; then
                    continue
                fi
                stat=$(ps -p $pid -o stat=)
                if [ "${stat:0:1}"x = "D"x ]; then
                    bfs_status["chunkserver$index"]="D"
                    if test $1 ; then
                        echo chunkserver$index" is Dead"
                    fi
                elif [ "${stat:0:1}"x = "Z"x ]; then
                    bfs_status["chunkserver$index"]="Z"
                    if test $1 ; then
                        echo chunkserver$index" is Zomble"
                    fi
                else
                    bfs_status["chunkserver$index"]="R"
                    if test $1 ; then
                        echo chunkserver$index" is Running"
                    fi
                fi
            fi
        else
            break
        fi
    done
}

function get_process_status
{
    pid=$1
    stat=$(ps -p $pid -o stat=)
    if [ $pid -eq 0 ]; then
        return 1
    elif [ "$stat"x = ""x ]; then
        return 1
    elif [ "${stat:0:1}"x = "D"x ]; then
        return 1
    elif [ "${stat:0:1}"x = "Z"x ]; then
        return 1
    fi
    return 0
 }

case $1 in
start)
    get_bfs_status ""
    if [ ${#bfs_status[*]} -ne 0 ]; then
        echo "BFS is already running..."
        exit
    fi

    ns_num=1
    if [ "$2"x = "raft"x ]; then
        ns_num=3;
    elif [ "$2"x == "master_slave"x ]; then
        ns_num=0;
    fi

    for((i=0;i<$ns_num;i++))
    do
        cd nameserver$i;
        ./bin/nameserver --node_index=$i 1>nlog 2&>1 &
        echo $! > pid
        get_process_status $(cat pid)
        if [ $? -eq 1 ]; then
            echo "start nameserver failed, exiting ..."
        fi
        cd - > /dev/null
    done;

    if [ "$1"x == "master_slave"x ]; then
        cd nameserver0;
        ./bin/nameserver --master_slave_role=slave --node_index=0 1>nlog 2>&1 &
        echo $! > pid
        get_process_status $(cat pid)
        if [ $? -eq 1 ]; then
            echo "start nameserver failed, exiting ..."
        fi
        cd - > /dev/null
        sleep 1
        cd nameserver1;
        ./bin/nameserver --master_slave_role=master --node_index=1 1>nlog 2>&1 &
        echo $! > pid
        get_process_status $(cat pid)
        if [ $? -eq 1 ]; then
            echo "start nameserver failed, exiting ..."
        fi
        cd - > /dev/null
    fi

    echo "starting BFS nameserver success"

    for i in `seq 0 3`;
    do
        cd chunkserver$i;
        ./bin/chunkserver --chunkserver_port=802$i 1>clog1 2>&1 &
        echo $! > pid
        get_process_status $(cat pid)
        if [ $? -eq 1 ]; then
            echo "start nameserver failed, exiting ..."
        fi
        cd - > /dev/null
    done
    echo "starting BFS chunkserver success"
    ;;
stop)
    #stop nameserver
    for((index=0;;index++));
    do
        if [ -x nameserver$index ]; then
            if [ ! -f nameserver$index/pid ]; then
                echo nameserver$index" stop failed (could not find file "nameserver$index/pid")"
            else
                pid=$(cat nameserver$index/pid)
                if [ $pid -ne 0 ]; then
                    $KILL -9 $(cat nameserver$index/pid)
                    echo 0 > nameserver$index/pid
                    echo "stop "nameserver$index" success "
                else
                    echo nameserver$index " is not running"
                fi
            fi
        else
            break
        fi
    done
    #stop chunkserver
    for((index=0;;index++));
    do
        if [ -x chunkserver$index ]; then
            if [ ! -f chunkserver$index/pid ]; then
                echo chunkserver$index" stop failed (could not find file "chunkserver$index/pid")"
            else
                pid=$(cat chunkserver$index/pid)
                if [ $pid -ne 0 ]; then
                    $KILL -9 $pid
                    echo 0 > chunkserver$index/pid
                    echo "stop "chunkserver$index" success "
                else
                    echo chunkserver$index " is not running"
                fi
             fi
        else
            break
        fi
    done
    ;;
status)
    get_bfs_status print
    if [ ${#bfs_status[*]} -eq 0 ]; then
        echo "BFS is Stoped"
        exit
    fi
    ;;
test)
    get_process_status 65566
    echo $?
    ;;
*)
    echo "Usage: $0 {start|stop|status}" >&2

esac
