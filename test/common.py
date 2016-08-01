"""
Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
"""

import subprocess
import filecmp
import os
import time
import nose.tools
import json
import commands
import string

from conf import const

def check_core():
    ret = 0
    (ret1, out1, err1) = runcmd("find %s -name \"core.*\" | grep \"core.*\"" % (const.bfs_nameserver_dir))
    if(ret1 == 0):
        runcmd("mkdir -p ./coresave/nameserver && find %s -name \"core.*\" | xargs -i mv {} ./coresave/nameserver" % (const.bfs_nameserver_dir))
        ret = 1
    (ret2, out2, err2) = runcmd("find %s -name \"core.*\" | grep \"core.*\"" % (const.bfs_chunkserver_dir))
    if(ret2 == 0):
        runcmd("mkdir -p ./coresave/chunkserver && find %s -name \"core.*\" | xargs -i mv {} ./coresave/chunkserver" % (const.bfs_chunkserver_dir))
        ret = 1
    (ret3, out3, err3) = runcmd("find %s -name \"core.*\" -maxdepth 1 | grep \"core.*\"" % (const.bfs_client_dir))
    if(ret3 == 0):
        runcmd("mkdir -p ./coresave/client && find %s -name \"core.*\" -maxdepth 1 | xargs -i mv {} ./coresave/client" % (const.bfs_client_dir))
        ret = 1
    return ret

def runcmd(cmd):
    """
    run cmd and return ret,out,err
    """
    print time.strftime("%Y%m%d-%H%M%S") + " command: "+cmd
    p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
    (out, err)=p.communicate()
    print "stdout: ", out
    print "stderr: ", err
    print "returncode: %d" % p.returncode
    ret = p.returncode
    return ret, out, err

def bfs_deploy():
    print "deploy bfs"
    ret = subprocess.Popen(const.deploy_script, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    print ''.join(ret.stdout.readlines())
    print ''.join(ret.stderr.readlines())

def bfs_start():
    print "start bfs"
    ret = subprocess.Popen(const.start_script, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    print ''.join(ret.stdout.readlines())
    print ''.join(ret.stderr.readlines())

def bfs_clear():
    ret = subprocess.Popen(const.clear_script, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    print ''.join(ret.stdout.readlines())
    print ''.join(ret.stderr.readlines())

def bfs_stop_all():
    print "stop bfs"
    (ret, out, err) = runcmd('killall -9 nameserver')
    (ret, out, err) = runcmd('killall -9 chunkserver')
    (ret, out, err) = runcmd('killall -9 bfs_client')

def check_process(type = all):
    ret = 0
    if type == "all" or type == "nameserver" :
        output = commands.getoutput("find %s -name pid | xargs -i cat {}" % const.bfs_nameserver_dir)
        pid_array = output.split('\n')
        for i in range(len(pid_array)):
            pid = pid_array[i]
            ret1 = os.system('ps x | grep %s | grep -v "grep"' % pid)
            if ret1 != 0:
                print "nameserver process %s not exist " % pid
                ret = 1
    if type == "all" or type == "chunkserver" :
        output = commands.getoutput("find %s -name pid | xargs -i cat {}" % const.bfs_chunkserver_dir)
        pid_array = output.split('\n')
        for i in range(len(pid_array)):
            pid = pid_array[i]
            ret1 = os.system('ps x | grep %s | grep -v "grep"' % pid)
            if ret1 != 0:
                print "chunkserver process %s not exist " % pid
                ret = 1 
    return ret

def modify_conf(file, key, value):
    (ret, out, err) = runcmd("sed -i 's/%s.*$/%s=%s/g' %s" % (key, key, value, file))
    assert(ret == 0)

def check_log(file, str):
    (ret, out, err) = runcmd("grep %s %s" % (str, file))
    return ret, out, err

