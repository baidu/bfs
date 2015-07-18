#! /bin/sh
set -e
set -x

sh ./clear.sh
sh ./deploy.sh
sh ./start_bfs.sh

sleep 3

./bfs_client ls /

./bfs_client put ./bfs_client /bfs_client

./bfs_client put ./bfs_client /bfs_client

./bfs_client mkdir /bin

./bfs_client mv /bfs_client /bin/bfs_client

./bfs_client get /bin/bfs_client ./binary

diff ./bfs_client ./binary > /dev/null

rm -rf ./binary

./bfs_client ls /

./bfs_client mkdir /home/user

./bfs_client touchz /home/user/flag

./bfs_client ls /home/user

./bfs_client rmr /home/user

./bfs_client ls /home

# Now we can list a nonexistent item
./bfs_client ls /home/user

touch empty_file1

./bfs_client put ./empty_file1 /ef

./bfs_client get /ef ./empty_file2

diff ./empty_file1 ./empty_file2 > /dev/null

rm -rf empty_file*

kill -9 `cat chunkserver0/pid`
kill -9 `cat chunkserver1/pid`

./bfs_client get /bin/bfs_client ./binary

rm -rf ./binary

echo "Test done!"
