#!/usr/bin/env bash
set -x
set -o pipefail
export PS4='+{$LINENO `date "+%Y-%m-%d_%H:%M:%S"` :${FUNCNAME[0]}}    '
cur=`dirname "${0}"`
cd "${cur}"
cur=`pwd`

killall -9 nameserver
killall -9 chunkserver
killall -9 bfs_client

rm -rf nameserver* chunkserver*
rm -rf master* slave*
rm -rf bfs_client
rm -rf bfs.flag
rm -rf client.*
rm -rf core.*
