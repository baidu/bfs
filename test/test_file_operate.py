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
    test touch new file method
    '''
def test_touch_new_file_1():

    cmd = "%s/bfs_client touchz /new_file_1" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)

    '''
    test touch new file method
    '''
def test_touch_new_file_2():

    cmd = "%s/bfs_client touchz /new_dir/new_file_2" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)

    '''
    test file exist method
    '''
def test_touch_file_exist():

    cmd = "%s/bfs_client touchz /new_file_3" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    assert(ret == 0)

    cmd = "%s/bfs_client touchz /new_file_3" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_not_equal(ret, 0)
    ret1 = common.check_process()
    nose.tools.assert_equal(ret1, 0)

    '''
    test list file method
    '''
def test_list_file_exist():

    cmd = "%s/bfs_client ls /new_file_1" % const.bfs_client_dir
    (ret1, out1, err1) = common.runcmd(cmd)
    nose.tools.assert_equal(ret1, 0)
    nose.tools.assert_equal(out1[0:14], "Found 1 items\n")

    cmd = "%s/bfs_client ls /new_dir/new_file_2" % const.bfs_client_dir
    (ret2, out2, err2) = common.runcmd(cmd)
    nose.tools.assert_equal(ret2, 0)
    nose.tools.assert_equal(out2[0:14], "Found 1 items\n")

    cmd = "%s/bfs_client ls /new_file_3" % const.bfs_client_dir
    (ret3, out3, err3) = common.runcmd(cmd)
    nose.tools.assert_equal(ret3, 0)
    nose.tools.assert_equal(out3[0:14], "Found 1 items\n")

    '''
    test list file method
    '''
def test_list_file_not_exist():

    cmd = "%s/bfs_client ls /new_file_not_exist" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_not_equal(ret, 0)
    ret1 = common.check_process()
    nose.tools.assert_equal(ret1, 0)

    '''
    test put file method
    '''
def test_put_file_dest_not_exist():

    cmd = "%s/bfs_client put %s/data/urllist /put_file_1" % (const.bfs_client_dir, const.work_dir)
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)

    '''
    test put file method
    '''
def test_put_file_destpath_is_dir():

    cmd = "%s/bfs_client mkdir /test_put_file" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    assert(ret == 0)

    cmd = "%s/bfs_client put %s/data/urllist /test_put_file" % (const.bfs_client_dir, const.work_dir)
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)
    
    cmd = "%s/bfs_client ls /test_put_file/urllist" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)

    '''
    test put file method
    '''
def test_put_empty_file():

    cmd = "%s/bfs_client put %s/data/empty_file /empty_file" % (const.bfs_client_dir, const.work_dir)
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)
    ret1 = common.check_process()
    nose.tools.assert_equal(ret1, 0)

    '''
    test put file method
    '''
def test_put_file_dest_exist():

    cmd = "%s/bfs_client put %s/data/file_not_exist /put_file_2" % (const.bfs_client_dir, const.work_dir)
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_not_equal(ret, 0)
    ret1 = common.check_process()
    nose.tools.assert_equal(ret1, 0)

    '''
    test put file method
    '''
def test_put_file_localfile_not_exist():

    cmd = "%s/bfs_client put %s/data/file_not_exist /file_not_exist" % (const.bfs_client_dir, const.work_dir)
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_not_equal(ret, 0)
    ret1 = common.check_process()
    nose.tools.assert_equal(ret1, 0)

    '''
    test cat file method
    '''
def test_cat_file_exist():

    cmd = "cat %s/data/urllist" % const.work_dir
    (ret1, out1, err1) = common.runcmd(cmd)
    assert(ret1 == 0)

    cmd = "%s/bfs_client cat /put_file_1" % const.bfs_client_dir
    (ret2, out2, err2) = common.runcmd(cmd)
    nose.tools.assert_equal(ret2, 0)
    nose.tools.assert_equal(out1, out2)

    '''
    test cat file method
    '''
def test_cat_file_not_exist():

    cmd = "%s/bfs_client cat /file_not_exist" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_not_equal(ret, 0)
    ret1 = common.check_process()
    nose.tools.assert_equal(ret1, 0)

    '''
    test cat file method
    '''
def test_cat_empty_file():

    cmd = "%s/bfs_client cat /empty_file" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)
    ret1 = common.check_process()
    nose.tools.assert_equal(ret1, 0)

    '''
    test get file method
    '''
def test_get_file_local_not_exist():

    cmd = "rm -rf %s/data/put_file_1 && %s/bfs_client get /put_file_1 %s/data/put_file_1" % (const.work_dir, const.bfs_client_dir, const.work_dir)
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)

    '''
    test get file method
    '''
def test_get_file_localfile_exist():

    cmd = "%s/bfs_client get /put_file_1 %s/data/put_file_1" % (const.bfs_client_dir, const.work_dir)
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_not_equal(ret, 0)
    ret1 = common.check_process()
    nose.tools.assert_equal(ret1, 0)

    '''
    test get file method
    '''
def test_get_file_bfsfile_not_exist():

    cmd = "%s/bfs_client get /not_exist_file %s/data/not_exist_file" % (const.bfs_client_dir, const.work_dir)
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_not_equal(ret, 0)
    ret1 = common.check_process()
    nose.tools.assert_equal(ret1, 0)

    '''
    test move(rename) file method
    '''
def test_move_file_diffdir_samename():

    cmd = "%s/bfs_client put %s/data/README.md /README.md" % (const.bfs_client_dir, const.work_dir)
    (ret, out, err) = common.runcmd(cmd)
    assert(ret == 0)
    cmd = "%s/bfs_client mkdir /test_move_file_1" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    assert(ret == 0)

    cmd = "%s/bfs_client mv /README.md /test_move_file_1/README.md" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)


    '''
    test move(rename) file method
    '''
def test_move_file_diffdir_diffname():

    cmd = "%s/bfs_client mkdir /test_move_file_2" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    assert(ret == 0)
    cmd = "%s/bfs_client mkdir /test_move_file_3" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    assert(ret == 0)
    cmd = "%s/bfs_client put %s/data/README.md /test_move_file_2/README.md" % (const.bfs_client_dir, const.work_dir)
    (ret, out, err) = common.runcmd(cmd)
    assert(ret == 0)

    cmd = "%s/bfs_client mv /test_move_file_2/README.md /test_move_file_3/README.md.new" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)


    '''
    test move(rename) file method
    '''
def test_move_file_samedir_diffname():

    cmd = "%s/bfs_client put %s/data/README.md /README.md" % (const.bfs_client_dir, const.work_dir)
    (ret, out, err) = common.runcmd(cmd)
    assert(ret == 0)

    cmd = "%s/bfs_client mv /README.md /README.md.new" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)


    '''
    test move(rename) file method
    '''
def test_move_file_samedir_samename():

    cmd = "%s/bfs_client mkdir /test_move_file_4" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)

    cmd = "%s/bfs_client put %s/data/urllist /test_move_file_4/urllist" % (const.bfs_client_dir, const.work_dir)
    (ret, out, err) = common.runcmd(cmd)
    assert(ret == 0)

    cmd = "%s/bfs_client mv /test_move_file_4/urllist /test_move_file_4/urllist" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)

    cmd = "cat %s/data/urllist" % const.work_dir
    (ret1, out1, err1) = common.runcmd(cmd)
    assert(ret1 == 0)

    cmd = "%s/bfs_client cat /test_move_file_4/urllist" % const.bfs_client_dir
    (ret2, out2, err2) = common.runcmd(cmd)
    nose.tools.assert_equal(ret2, 0)
    nose.tools.assert_equal(out1, out2)


    '''
    test move(rename) file method
    '''
def test_move_file_srcpath_not_exist():

    cmd = "%s/bfs_client mv /file_not_exist /file_not_exist.new" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_not_equal(ret, 0)
    ret1 = common.check_process()
    nose.tools.assert_equal(ret1, 0)


    '''
    test move(rename) file method
    '''
def test_move_file_destfile_is_exist():

    cmd = "%s/bfs_client put %s/data/README.md /README.md" % (const.bfs_client_dir, const.work_dir)
    (ret, out, err) = common.runcmd(cmd)
    assert(ret == 0)
    cmd = "%s/bfs_client touchz /README.md.1" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    assert(ret == 0)

    cmd = "%s/bfs_client mv /README.md /README.md.1" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_not_equal(ret, 0)
    ret1 = common.check_process()
    nose.tools.assert_equal(ret1, 0)


    '''
    test move(rename) file method
    '''
def test_move_file_destpath_is_dir():

    cmd = "%s/bfs_client put %s/data/README.md /test_move_file" % (const.bfs_client_dir, const.work_dir)
    (ret, out, err) = common.runcmd(cmd)
    assert(ret == 0)
    cmd = "%s/bfs_client mkdir /test_move_dir" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    assert(ret == 0)

    cmd = "%s/bfs_client mv /test_move_file /test_move_dir" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)
    cmd = "%s/bfs_client ls /test_move_dir/test_move_file" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)


    '''
    test remove file method
    '''
def test_remove_file_exist():

    cmd = "%s/bfs_client mkdir /test_remove_file/test_dir" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    assert(ret == 0)
    cmd = "%s/bfs_client put %s/data/binfile /test_remove_file/test_dir/binfile" % (const.bfs_client_dir, const.work_dir)
    (ret, out, err) = common.runcmd(cmd)
    assert(ret == 0)

    cmd = "%s/bfs_client rm /test_remove_file/test_dir/binfile" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)


    '''
    test remove file method
    '''
def test_remove_file_not_exist():

    cmd = "%s/bfs_client rm /test_remove_file/test_dir/file_not_exist" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_not_equal(ret, 0)
    ret1 = common.check_process()
    nose.tools.assert_equal(ret1, 0)

    '''
    test du file method
    '''
def test_du_file_exist():

    cmd = "%s/bfs_client mkdir /test_du_file_1" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    assert(ret == 0)
    cmd = "%s/bfs_client put %s/data/urllist /test_du_file_1/urllist" % (const.bfs_client_dir, const.work_dir)
    (ret, out, err) = common.runcmd(cmd)
    assert(ret == 0)
    cmd = "%s/bfs_client put %s/data/binfile /test_du_file_1/binfile" % (const.bfs_client_dir, const.work_dir)
    (ret, out, err) = common.runcmd(cmd)
    assert(ret == 0)

    cmd = "%s/bfs_client du /test_du_file_1/urllist" % const.bfs_client_dir
    (ret1, out1, err1) = common.runcmd(cmd)
    nose.tools.assert_equal(ret1, 0)
    nose.tools.assert_equal(out1, "/test_du_file_1/urllist\t40\nTotal:\t40\n")
    
    cmd = "%s/bfs_client du /test_du_file_2/binfile" % const.bfs_client_dir
    (ret2, out2, err2) = common.runcmd(cmd)
    nose.tools.assert_equal(ret2, 0)
    nose.tools.assert_equal(out2, "/test_du_file_1/binfile\t24\nTotal:\t24\n")
    


def test_du_empty_file():

    cmd = "%s/bfs_client mkdir /test_du_file_2" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    assert(ret == 0)
    cmd = "%s/bfs_client put %s/data/empty_file /test_du_file_2/empty_file" % (const.bfs_client_dir, const.work_dir)
    (ret, out, err) = common.runcmd(cmd)
    assert(ret == 0)

    cmd = "%s/bfs_client du /test_du_file_2/empty_file" % const.bfs_client_dir
    (ret, out, err) = common.runcmd(cmd)
    nose.tools.assert_equal(ret, 0)
    nose.tools.assert_equal(out, "/test_du_file_2/empty_file\t0\nTotal:\t0\n")



