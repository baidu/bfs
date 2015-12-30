#! /bin/sh
set -e
set -x

sh ./clear.sh
sh ./deploy.sh
sh ./start_bfs.sh

sleep 3

# Test sl
./bfs_client ls /

# File put
./bfs_client put ./bfs_client /bfs_client

# File put rewrite
./bfs_client put ./bfs_client /bfs_client

# Test mkdir
./bfs_client mkdir /bin

# Test move
./bfs_client mv /bfs_client /bin/bfs_client

echo Test atomic rename
./bfs_client put ./bfs_client /bfs_client
./bfs_client mv /bfs_client /bin/bfs_client

# Test get
./bfs_client get /bin/bfs_client ./binary

diff ./bfs_client ./binary > /dev/null

rm -rf ./binary

# More test for base operations
./bfs_client ls /

./bfs_client mkdir /home/user

./bfs_client touchz /home/user/flag

./bfs_client ls /home/user

# Test rmr
./bfs_client rmr /home/user

./bfs_client ls /home

# Now we can list a nonexistent item
#./bfs_client ls /home/user

# Put & get empty file
touch empty_file1

./bfs_client put ./empty_file1 /ef

./bfs_client get /ef ./empty_file2

diff ./empty_file1 ./empty_file2 > /dev/null

rm -rf empty_file*

# Put more files
for i in `ls ../src/`;
do
    if [ -d ../src/$i ]
    then
        for j in `ls ../src/$i`;
        do
            ./bfs_client put ../src/$i/$j /home/src/$i/$j
        done;
    else
        ./bfs_client put ../src/$i /home/src/$i
    fi
done;

# Kill chunkserver and test retry
kill -9 `cat chunkserver0/pid`
kill -9 `cat chunkserver1/pid`

./bfs_client get /bin/bfs_client ./binary
rm -rf ./binary

# Nameserver restart
kill -9 `cat nameserver/pid`
cd nameserver
./bin/nameserver 1>nlog2 2>&1 &
sleep 3
cd -
./bfs_client get /bin/bfs_client ./binary
rm -rf ./binary

echo "Test done!"
