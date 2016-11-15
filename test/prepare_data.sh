#!/usr/bin/env bash
set -x
set -o pipefail
export PS4='+{$LINENO `date "+%Y-%m-%d_%H:%M:%S"` :${FUNCNAME[0]}}    '
cur=`dirname "${0}"`
cd "${cur}"
cur=`pwd`

mkdir -p ./data

cd ./data

cp ../../README.md ./
[ $? -ne 0 ] && exit 1

touch ./empty_file
[ $? -ne 0 ] && exit 1

echo -e "https://github.com/\nhttps://github.com/" > ./urllist
[ $? -ne 0 ] && exit 1

python ../gen_binfile.py
[ $? -ne 0 ] && exit 1

cd ..

exit 0

