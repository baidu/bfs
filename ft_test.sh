set -x -e

make install;
cd sandbox; sh small_test.sh; sh clear.sh; sh deploy.sh; sh start_bfs.sh;
cd ../tera; make clean; make; ./bfs_test; cd ../sandbox; sh clear.sh
