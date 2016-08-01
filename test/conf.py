"""
Copyright (c) 2016, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
"""


class Const:
    def __init__(self):
        self.deploy_script = './deploy.sh'
        self.clear_script = './clear.sh'
        self.start_script = './start_bfs.sh'
        self.restart_script = './restart_bfs.sh'
        self.stop_script = './stop_bfs.sh'
        self.bfs_client = './bfs_client'
        self.work_dir = './'
        self.bfs_client_dir = './'
        self.bfs_nameserver_dir = './nameserver*'
        self.bfs_chunkserver_dir = './chunkserver*'

const = Const()
