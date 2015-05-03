#! /bin/sh
set -e
set -x

sh ./clear.sh
sh ./deploy.sh

ldd bfs_client
ls -l /usr/local/lib/libsnappy.so.1

sh ./start_bfs.sh

sleep 3

./bfs_client ls /

./bfs_client put ./bfs_client /bfs_client

./bfs_client mkdir /bin

./bfs_client mv /bfs_client /bin/bfs_client

./bfs_client get /bin/bfs_client ./binary

rm -rf ./binary

./bfs_client ls /

./bfs_client mkdir /home/user

./bfs_client touchz /home/user/flag

./bfs_client ls /home/user

./bfs_client rmr /home/user

./bfs_client ls /home

# Now we can list a nonexistent item
./bfs_client ls /home/user

