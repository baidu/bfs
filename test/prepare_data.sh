#!/bin/bash

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

