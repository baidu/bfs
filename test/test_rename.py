'''
Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
'''

import common 
    
def setUp():
    common.bfs_deploy()
    common.print_debug_msg(1, "deploy nameserver, chunkserver and status is ok")
    common.bfs_start()
    common.print_debug_msg(1, "start nameserver, chunkserver and status is ok")

def tearDown():
    common.bfs_stop_all()
    common.print_debug_msg(1, "stop nameserver, chunkserver and status is ok")
    common.bfs_clear()
    common.print_debug_msg(1, "clear data and status is ok")
    
    '''
    rename file method
    '''
def test_rename_file():

    common.print_debug_msg(2, "test rename file")

    cmd = "./bfs_client put ./bfs_client /bfs_client"
    common.runcmd(cmd)

    cmd = './bfs_client mv /bfs_client /bfs_client_new'
    common.runcmd(cmd)

