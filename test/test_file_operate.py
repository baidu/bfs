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
    
    '''
    test touch new file method
    '''
def test_touch_new_file_1():

    cmd = "./bfs_client touchz /new_file_1"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_equal(ret, 0)
    return ret

    '''
    test touch new file method
    '''
def test_touch_new_file_2():

    cmd = "./bfs_client touchz /new_dir/new_file_2"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_equal(ret, 0)
    return ret

    '''
    test file exist method
    '''
def test_touch_file_exist():

    cmd = "./bfs_client touchz /new_file_3"
    ret = common.runcmd(cmd, ignore_status=False)

    cmd = "./bfs_client touchz /new_file_3"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_not_equal(ret, 0)
    ret1 = common.check_core()
    nose.tools.assert_equal(ret1, 0)
    return ret+ret1

    '''
    test list file method
    '''
def test_list_file_exist():

    cmd = "./bfs_client ls /new_file_1"
    p1 = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out1, err1) = p1.communicate()
    ret1 = p1.returncode
    print "./bfs_client ls /new_file_1 :"
    print out1
    nose.tools.assert_equal(ret1, 0)
    nose.tools.assert_equal(out1[0:14], "Found 1 items\n")

    cmd = "./bfs_client ls /new_dir/new_file_2"
    p2 = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out2, err2) = p2.communicate()
    ret2 = p2.returncode
    print "./bfs_client ls /new_dir/new_file_2 :"
    print out2
    nose.tools.assert_equal(ret2, 0)
    nose.tools.assert_equal(out2[0:14], "Found 1 items\n")

    cmd = "./bfs_client ls /new_file_3"
    p3 = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out3, err3) = p3.communicate()
    ret3 = p3.returncode
    print "./bfs_client ls /new_file_3 :"
    print out3
    nose.tools.assert_equal(ret3, 0)
    nose.tools.assert_equal(out3[0:14], "Found 1 items\n")

    return ret1+ret2+ret3

    '''
    test list file method
    '''
def test_list_file_not_exist():

    cmd = "./bfs_client ls /new_file_not_exist"
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out, err) = p.communicate()
    ret = p.returncode
    print "./bfs_client ls /new_file_not_exist :"
    print out
    nose.tools.assert_not_equal(ret, 0)
    ret1 = common.check_core()
    nose.tools.assert_equal(ret1, 0)

    return ret+ret1

    '''
    test put file method
    '''
def test_put_file_dest_not_exist():

    cmd = "./bfs_client put ./data/urllist /put_file_1"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_equal(ret, 0)
    return ret

    '''
    test put file method
    '''
def test_put_file_destpath_is_dir():

    cmd = "./bfs_client mkdir /test_put_file"
    common.runcmd(cmd, ignore_status=False)

    cmd = "./bfs_client put ./data/urllist /test_put_file"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_equal(ret, 0)
    
    cmd = "./bfs_client ls /test_put_file/urllist"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_equal(ret, 0)

    return ret

    '''
    test put file method
    '''
def test_put_empty_file():

    cmd = "./bfs_client put ./data/empty_file /empty_file"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_equal(ret, 0)
    ret1 = common.check_core()
    nose.tools.assert_equal(ret1, 0)
    return ret+ret1

    '''
    test put file method
    '''
def test_put_file_dest_exist():

    cmd = "./bfs_client put ./data/file_not_exist /put_file_2"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_not_equal(ret, 0)
    ret1 = common.check_core()
    nose.tools.assert_equal(ret1, 0)
    return ret+ret1

    '''
    test put file method
    '''
def test_put_file_localfile_not_exist():

    cmd = "./bfs_client put ./data/file_not_exist /file_not_exist"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_not_equal(ret, 0)
    ret1 = common.check_core()
    nose.tools.assert_equal(ret1, 0)
    return ret+ret1

    '''
    test cat file method
    '''
def test_cat_file_exist():

    cmd = "cat ./data/urllist"
    print time.strftime("%Y%m%d-%H%M%S") + " command: "+cmd
    p1 = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out1, err1) = p1.communicate()
    print "cat ./data/urllist stdout: "
    print out1

    cmd = "./bfs_client cat /put_file_1"
    print time.strftime("%Y%m%d-%H%M%S") + " command: "+cmd
    p2 = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out2, err2) = p2.communicate()
    print "cat bfs /put_file_1 stdout: "
    print out2
    ret = p2.returncode
    nose.tools.assert_equal(ret, 0)
    nose.tools.assert_equal(out1, out2)
    return ret

    '''
    test cat file method
    '''
def test_cat_file_exist():

    cmd = "./bfs_client cat /file_not_exist"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_not_equal(ret, 0)
    common.check_core()
    return ret

    '''
    test cat file method
    '''
def test_cat_empty_file():

    cmd = "./bfs_client cat /empty_file"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_equal(ret, 0)
    ret1 = common.check_core()
    nose.tools.assert_equal(ret1, 0)
    return ret+ret1

    '''
    test get file method
    '''
def test_get_file_local_not_exist():

    cmd = "rm -rf ./data/put_file_1 && ./bfs_client get /put_file_1 ./data/put_file_1"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_equal(ret, 0)
    return ret

    '''
    test get file method
    '''
def test_get_file_localfile_exist():

    cmd = "./bfs_client get /put_file_1 ./data/put_file_1"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_not_equal(ret, 0)
    ret1 = common.check_core()
    nose.tools.assert_equal(ret1, 0)
    return ret+ret1

    '''
    test get file method
    '''
def test_get_file_bfsfile_not_exist():

    cmd = "./bfs_client get /not_exist_file ./data/not_exist_file"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_not_equal(ret, 0)
    ret1 = common.check_core()
    nose.tools.assert_equal(ret1, 0)
    return ret+ret1

    '''
    test move(rename) file method
    '''
def test_move_file_diffdir_samename():

    cmd = "./bfs_client put ./data/README.md /README.md"
    common.runcmd(cmd, ignore_status=False)
    cmd = "./bfs_client mkdir /test_move_file_1"
    common.runcmd(cmd, ignore_status=False)

    cmd = "./bfs_client mv /README.md /test_move_file_1/README.md"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_equal(ret, 0)
    return ret


    '''
    test move(rename) file method
    '''
def test_move_file_diffdir_diffname():

    cmd = "./bfs_client mkdir /test_move_file_2"
    common.runcmd(cmd, ignore_status=False)
    cmd = "./bfs_client mkdir /test_move_file_3"
    common.runcmd(cmd, ignore_status=False)
    cmd = "./bfs_client put ./data/README.md /test_move_file_2/README.md"
    common.runcmd(cmd, ignore_status=False)

    cmd = "./bfs_client mv /test_move_file_2/README.md /test_move_file_3/README.md.new"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_equal(ret, 0)
    return ret


    '''
    test move(rename) file method
    '''
def test_move_file_samedir_diffname():

    cmd = "./bfs_client put ./data/README.md /README.md"
    common.runcmd(cmd, ignore_status=False)

    cmd = "./bfs_client mv /README.md /README.md.new"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_equal(ret, 0)
    return ret


    '''
    test move(rename) file method
    '''
def test_move_file_srcpath_not_exist():

    cmd = "./bfs_client mv /file_not_exist /file_not_exist.new"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_not_equal(ret, 0)
    ret1 = common.check_core()
    nose.tools.assert_equal(ret1, 0)

    return ret+ret1


    '''
    test move(rename) file method
    '''
def test_move_file_destfile_is_exist():

    cmd = "./bfs_client put ./data/README.md /README.md"
    common.runcmd(cmd, ignore_status=False)
    cmd = "./bfs_client touchz /README.md.1"
    common.runcmd(cmd, ignore_status=False)

    cmd = "./bfs_client mv /README.md /README.md.1"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_not_equal(ret, 0)
    ret1 = common.check_core()
    nose.tools.assert_equal(ret1, 0)

    return ret+ret1

    '''
    test move(rename) file method
    '''
def test_move_file_destpath_is_dir():

    cmd = "./bfs_client put ./data/README.md /test_move_file"
    common.runcmd(cmd, ignore_status=False)
    cmd = "./bfs_client mkdir /test_move_dir"
    common.runcmd(cmd, ignore_status=False)

    cmd = "./bfs_client mv /test_move_file /test_move_dir"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_equal(ret, 0)
    cmd = "./bfs_client ls /test_move_dir/test_move_file"
    ret1 = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_equal(ret1, 0)

    return ret+ret1

    '''
    test remove file method
    '''
def test_remove_file_exist():

    cmd = "./bfs_client mkdir /test_remove_file/test_dir"
    common.runcmd(cmd, ignore_status=False)
    cmd = "./bfs_client put ./data/mark /test_remove_file/test_dir/mark"
    common.runcmd(cmd, ignore_status=False)

    cmd = "./bfs_client rm /test_remove_file/test_dir/mark"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_equal(ret, 0)
    return ret


    '''
    test remove file method
    '''
def test_remove_file_not_exist():

    cmd = "./bfs_client rm /test_remove_file/test_dir/file_not_exist"
    ret = common.runcmd(cmd, ignore_status=True)
    nose.tools.assert_not_equal(ret, 0)
    ret1 = common.check_core()
    nose.tools.assert_equal(ret1, 0)
    return ret+ret1

    '''
    test du file method
    '''
def test_du_file_exist():

    cmd = "./bfs_client mkdir /test_du_file_1"
    common.runcmd(cmd, ignore_status=False)
    cmd = "./bfs_client put ./data/urllist /test_du_file_1/urllist"
    common.runcmd(cmd, ignore_status=False)
    cmd = "./bfs_client put ./data/mark /test_du_file_1/mark"
    common.runcmd(cmd, ignore_status=False)

    cmd = "./bfs_client du /test_du_file_1/urllist"
    p1 = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out1, err1) = p1.communicate()
    ret1 = p1.returncode
    print "./bfs_client du /test_du_file_1/urllist :"
    print out1
    nose.tools.assert_equal(ret1, 0)
    nose.tools.assert_equal(out1, "/test_du_file_1/urllist\t40\nTotal:\t40\n")
    
    cmd = "./bfs_client du /test_du_file_2/mark"
    p2 = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out2, err2) = p2.communicate()
    ret2 = p2.returncode
    print "./bfs_client du /test_du_file_1/urllist :"
    print out2
    nose.tools.assert_equal(ret2, 0)
    nose.tools.assert_equal(out2, "/test_du_file_1/mark\t16166225\nTotal:\t16166225\n")
    
    return ret1+ret2


def test_du_empty_file():

    cmd = "./bfs_client mkdir /test_du_file_2"
    common.runcmd(cmd, ignore_status=False)
    cmd = "./bfs_client put ./data/empty_file /test_du_file_2/empty_file"
    common.runcmd(cmd, ignore_status=False)

    cmd = "./bfs_client du /test_du_file_2/empty_file"
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    (out, err) = p.communicate()
    ret = p.returncode
    print "./bfs_client du /test_du_file_2/empty_file :"
    print out
    nose.tools.assert_equal(ret, 0)
    nose.tools.assert_equal(out, "/test_du_file_2/empty_file\t0\nTotal:\t0\n")

    return ret


