#!/usr/bin/env bash
set -x
set -o pipefail
export PS4='+{$LINENO `date "+%Y-%m-%d_%H:%M:%S"` :${FUNCNAME[0]}}    '
cur=`dirname "${0}"`
cd "${cur}"
cur=`pwd`
set -e

make install;
make test; cd sandbox; sh clear.sh; sh deploy.sh; sh start_bfs.sh;
cd ../tera; make clean; make; ./bfs_test; cd ../sandbox; sh clear.sh
