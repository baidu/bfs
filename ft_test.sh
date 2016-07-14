set -x -e

make install;
make test; cd sandbox; sh clear.sh; sh deploy.sh; sh start_bfs.sh;
cd ../tera; make clean; make; ./bfs_test; cd ../sandbox; sh clear.sh
