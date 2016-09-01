'''
Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
'''

import subprocess
import time
import os
import nose.tools
import common

from conf import const
    
def setUp():
    common.bfs_clear()
    print "clear data and status is ok"
    common.bfs_deploy()
    print "deploy nameserver, chunkserver and status is ok"

def tearDown():
    ret = common.check_core()
    nose.tools.assert_equal(ret, 0) 
    common.bfs_stop_all()
    print "stop nameserver, chunkserver and status is ok"
    
    '''
    test nameserver start method
    '''
def test_nameserver_start_normal():

    cmd = "cd %s/nameserver0 && ./bin/nameserver 1>nlog 2>&1 &" % const.work_dir
    print time.strftime("%Y%m%d-%H%M%S") + " command: "+cmd
    ret = os.system(cmd)
    nose.tools.assert_equal(ret, 0)
    time.sleep(3)

    '''
    test nameserver stop method
    '''
def test_nameserver_stop_normal():

    cmd = "killall -9 nameserver"
    print time.strftime("%Y%m%d-%H%M%S") + " command: "+cmd
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)
    time.sleep(3)
    ret1 = common.check_process(type = "nameserver")
    nose.tools.assert_equal(ret1, 1)

    '''
    test nameserver restart method
    '''
def test_nameserver_restart_normal():

    common.bfs_stop_all()

    cmd = "cd %s/nameserver0 && ./bin/nameserver 1>nlog 2>&1 &" % const.work_dir
    print time.strftime("%Y%m%d-%H%M%S") + " command: "+cmd
    ret = os.system(cmd)
    nose.tools.assert_equal(ret, 0)
    time.sleep(3)
    ret1 = common.check_process(type = "nameserver")
    

    '''
    test nameserver start twice method
    '''
def test_nameserver_start_twice():

    cmd = "cd %s/nameserver0 && ./bin/nameserver 1>nlog 2>&1 &" % const.work_dir
    print time.strftime("%Y%m%d-%H%M%S") + " command: "+cmd
    ret = os.system(cmd)
    nose.tools.assert_not_equal(ret, 0)
    time.sleep(3)
    cmd = "%s/bfs_client touchz /test_nameserver_start_twice" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)
    ret1 = common.check_process(type = "nameserver")
    nose.tools.assert_equal(ret1, 0)

    '''
    test nameserver start conf not exist method
    '''
def test_nameserver_start_conf_not_exist():

    common.bfs_stop_all()
    
    (ret, out, err) = common.runcmd("cd nameserver0 && mv bfs.flag bfs.flag.bak")
    assert(ret == 0)

    cmd = "cd %s/nameserver0 && ./bin/nameserver 1>nlog 2>&1 &" % const.work_dir
    print time.strftime("%Y%m%d-%H%M%S") + " command: "+cmd
    ret = os.system(cmd)
    nose.tools.assert_not_equal(ret, 0)
    time.sleep(3)
    ret1 = common.check_process(type = "nameserver")
    nose.tools.assert_equal(ret1, 1)

    (ret, out, err) = common.runcmd("cd nameserver0 && mv bfs.flag.bak bfs.flag")
    assert(ret == 0)

