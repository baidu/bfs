'''
Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
'''

import subprocess
import time
import common 
import nose.tools
    
def setUp():
    common.bfs_clear()
    common.print_debug_msg(1, "clear data and status is ok")
    common.bfs_deploy()
    common.print_debug_msg(1, "deploy nameserver, chunkserver and status is ok")
    common.bfs_start()
    common.print_debug_msg(1, "start nameserver, chunkserver and status is ok")

def tearDown():
    common.bfs_stop_all()
    common.print_debug_msg(1, "stop nameserver, chunkserver and status is ok")
    #common.bfs_clear()
    #common.print_debug_msg(1, "clear data and status is ok")


    '''
    test read file not exist
    '''
def test_read_file_not_exist():
    
    common.print_debug_msg(2, "test read file not exist")
    
    cmd = "./mark --mode=read --count=10 --thread=3 --seed=1 --file_size=1024"
    ret1 = common.runcmd(cmd, ignore_status=True)
    ret2 = common.check_core()
    nose.tools.assert_not_equal(ret1, 0)
    nose.tools.assert_equal(ret2, 0)
    
    return ret1+ret2
    
    '''
    test put and read empty file
    '''
def test_put_empty_file():

    common.print_debug_msg(2, "test put empty file")

    cmd = "./mark --mode=put --count=10 --thread=3 --seed=1 --file_size=0"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_equal(ret, 0)
    return ret

def test_read_empty_file():
    
    common.print_debug_msg(2, "test read empty file")

    cmd = "./mark --mode=read --count=10 --thread=3 --seed=1 --file_size=0"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_equal(ret, 0)
    return ret

    '''
    test put and read 1M file
    '''
def test_put_1M_file():

    common.print_debug_msg(2, "test put 1024K file")

    cmd = "./mark --mode=put --count=10 --thread=3 --seed=2 --file_size=1024"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_equal(ret, 0)
    return ret

def test_read_1M_file():

    common.print_debug_msg(2, "test read 1024K file")

    cmd = "./mark --mode=read --count=10 --thread=3 --seed=2 --file_size=1024"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_equal(ret, 0)
    return ret

    '''
    test put and read 1G file
    '''
def test_put_read_1G_file():

    common.print_debug_msg(2, "test put 1G file")

    cmd = "./mark --mode=put --count=2 --thread=1 --seed=3 --file_size=1048576"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_equal(ret, 0)
    return ret

def test_read_1G_file():

    common.print_debug_msg(2, "test read 1G file")

    cmd = "./mark --mode=read --count=2 --thread=1 --seed=3 --file_size=1048576"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_equal(ret, 0)

    return ret


