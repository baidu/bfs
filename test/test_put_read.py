'''
Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
'''

import subprocess
import time
import common 
import nose.tools

from conf import const
    
def setUp():
    common.bfs_clear()
    print "clear data and status is ok"
    common.bfs_deploy()
    print "deploy nameserver, chunkserver and status is ok"
    common.bfs_start()
    print "start nameserver, chunkserver and status is ok"

def tearDown():
    ret = common.check_core()
    nose.tools.assert_equal(ret, 0)
    common.bfs_stop_all()
    print "stop nameserver, chunkserver and status is ok"


    '''
    test read file not exist
    '''
def test_read_file_not_exist():
    
    cmd = "%s/mark --mode=read --count=10 --thread=3 --seed=1 --file_size=1024" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    ret1 = common.check_process()
    nose.tools.assert_not_equal(ret, 0)
    nose.tools.assert_equal(ret1, 0)
    
    
    '''
    test put and read empty file
    '''
def test_put_empty_file():

    cmd = "%s/mark --mode=put --count=10 --thread=3 --seed=1 --file_size=0" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)

    '''
    test read empty file
    '''
def test_read_empty_file():
    
    cmd = "%s/mark --mode=read --count=10 --thread=3 --seed=1 --file_size=0" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)

    '''
    test put and read 1M file
    '''
def test_put_1M_file():

    cmd = "%s/mark --mode=put --count=10 --thread=3 --seed=2 --file_size=1024" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)

def test_read_1M_file():

    cmd = "%s/mark --mode=read --count=10 --thread=3 --seed=2 --file_size=1024" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)

    '''
    test put and read 1G file
    '''
def test_put_read_1G_file():

    cmd = "%s/mark --mode=put --count=2 --thread=1 --seed=3 --file_size=1048576" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)

def test_read_1G_file():

    cmd = "%s/mark --mode=read --count=2 --thread=1 --seed=3 --file_size=1048576" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)



