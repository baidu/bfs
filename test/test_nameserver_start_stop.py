'''
Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
'''

import subprocess
import time
import os
import nose.tools
import common
    
def setUp():
    common.bfs_clear()
    common.print_debug_msg(1, "clear data and status is ok")
    common.bfs_deploy()
    common.print_debug_msg(1, "deploy nameserver, chunkserver and status is ok")
    #common.bfs_start()
    #common.print_debug_msg(1, "start nameserver, chunkserver and status is ok")

def tearDown():
    common.bfs_stop_all()
    common.print_debug_msg(1, "stop nameserver, chunkserver and status is ok")
    
    '''
    test nameserver start method
    '''
def test_nameserver_start_normal():

    common.print_debug_msg(2, "test nameserver start")

    cmd = "cd nameserver0 && ./bin/nameserver 1>nlog 2>&1 &"
    print time.strftime("%Y%m%d-%H%M%S") + " command: "+cmd
    #p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
    #(out,err) = p.communicate()
    #print "stdout: "
    #print out
    #print "stderr: "
    #print err
    #print "returncode: %d" % p.returncode
    #ret = p.returncode
    ret = os.system(cmd)
    nose.tools.assert_equal(ret, 0)
    time.sleep(3)
    return ret

    '''
    test nameserver stop method
    '''
def test_nameserver_stop_normal():

    common.print_debug_msg(3, "test nameserver stop")

    cmd = "killall -9 nameserver"
    print time.strftime("%Y%m%d-%H%M%S") + " command: "+cmd
    ret = common.runcmd(cmd, ignore_status=True)
    #ret = os.system(cmd)
    nose.tools.assert_equal(ret, 0)
    time.sleep(3)
    ret1 = common.check_core()
    nose.tools.assert_equal(ret1, 0)
    return ret+ret1

    '''
    test nameserver restart method
    '''
def test_nameserver_restart_normal():

    common.print_debug_msg(3, "test nameserver restart")
    
    common.bfs_stop_all()
    common.print_debug_msg(3, "stop nameserver, chunkserver and status is ok")

    cmd = "cd nameserver0 && ./bin/nameserver 1>nlog 2>&1 &"
    print time.strftime("%Y%m%d-%H%M%S") + " command: "+cmd
    ret = os.system(cmd)
    nose.tools.assert_equal(ret, 0)
    time.sleep(3)
    common.check_core()
    return ret


    '''
    test nameserver start twice method
    '''
def test_nameserver_start_twice():

    common.print_debug_msg(4, "test nameserver start twice")

    cmd = "cd nameserver0 && ./bin/nameserver 1>nlog 2>&1 &"
    print time.strftime("%Y%m%d-%H%M%S") + " command: "+cmd
    ret = os.system(cmd)
    nose.tools.assert_not_equal(ret, 0)
    time.sleep(3)
    cmd = "./bfs_client ls /"
    ret1 = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_equal(ret1, 0)
    ret2 = common.check_core()
    nose.tools.assert_equal(ret2, 0)
    return ret+ret1+ret2

    '''
    test nameserver start conf not exist method
    '''
def test_nameserver_start_conf_not_exist():

    common.print_debug_msg(3, "test nameserver restart")

    common.bfs_stop_all()
    common.print_debug_msg(3, "stop nameserver, chunkserver and status is ok")
    
    common.runcmd("cd nameserver0 && mv bfs.flag bfs.flag.bak", ignore_status=False)

    cmd = "cd nameserver0 && ./bin/nameserver 1>nlog 2>&1 &"
    print time.strftime("%Y%m%d-%H%M%S") + " command: "+cmd
    ret = os.system(cmd)
    nose.tools.assert_equal(ret, 0)
    time.sleep(3)
    ret1 = common.check_core()
    nose.tools.assert_equal(ret1, 0)

    common.runcmd("cd nameserver0 && mv bfs.flag.bak bfs.flag", ignore_status=False)

    return ret+ret1

