"""
Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
"""

import subprocess
import filecmp
import os
import time
import nose.tools
import json

from conf import const

def check_core():
    ret = 0
    #ret = runcmd("cd %s && ls|grep core" % (const.bfs_nameserver_dir), ignore_status=True)
    ret1 = runcmd("find %s -name \"core.*\" | grep \"core.*\"" % (const.bfs_nameserver_dir), ignore_status=True)
    if(ret1 == 0):
        runcmd("mkdir -p ./coresave/nameserver && find %s -name \"core.*\" | xargs -i mv {} ./coresave/nameserver" % (const.bfs_nameserver_dir))
        ret = 1
    ret2 = runcmd("find %s -name \"core.*\" | grep \"core.*\"" % (const.bfs_chunkserver_dir), ignore_status=True)
    if(ret2 == 0):
        runcmd("mkdir -p ./coresave/chunkserver && find %s -name \"core.*\" | xargs -i mv {} ./coresave/chunkserver" % (const.bfs_chunkserver_dir))
        ret = 1
    ret3 = runcmd("find %s -name \"core.*\" -maxdepth 1 | grep \"core.*\"" % (const.bfs_client_dir), ignore_status=True)
    if(ret3 == 0):
        runcmd("mkdir -p ./coresave/client && find %s -name \"core.*\" -maxdepth 1 | xargs -i mv {} ./coresave/client" % (const.bfs_client_dir))
        ret = 1
    return ret

def runcmd(cmd, ignore_status=False):
    """
    run cmd and return code
    """
    print time.strftime("%Y%m%d-%H%M%S") + " command: "+cmd
    p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
    (out,err)=p.communicate()
    print "stdout: "
    print out
    print "stderr: "
    print err
    print "returncode: %d" % p.returncode
    ret = p.returncode
    if not ignore_status:
        assert(ret == 0)
        #nose.tools.assert_equal(ret, 0 )
    return ret

def runcmd_output(cmd, ignore_status=False):
    """
    run cmd and return code
    """
    print time.strftime("%Y%m%d-%H%M%S") + " command: "+cmd
    p = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True)
    (out,err)=p.communicate()
    print "stdout: "
    print out
    print "stderr: "
    print err
    print "returncode: %d" % p.returncode
    ret = p.returncode
    if not ignore_status:
        assert(ret == 0)
    return out.strip()

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
    ret = runcmd('killall -9 nameserver', ignore_status=True)
    ret = runcmd('killall -9 chunkserver', ignore_status=True)
    ret = runcmd('killall -9 bfs_client', ignore_status=True)


def wait_table_ready(tablename):
    timeout=10
    while( timeout > 0 ):
        time.sleep(2)
        timeout=timeout-1
        disable_count = runcmd_output('cd %s && ./teracli show %s|grep kTabletDisable|wc -l' % (const.teracli_dir, tablename), ignore_status=True)
        tablet_count = runcmd_output('cd %s && ./teracli show %s|grep %s|wc -l' % (const.teracli_dir, tablename, tablename), ignore_status=True)
        if ( disable_count == tablet_count ):
            return
    assert(timeout>0)

def drop_table(tablename):
    ret = runcmd('cd %s && ./teracli show %s' % (const.teracli_dir, tablename), ignore_status=True)
    if(ret == 0):
        runcmd('cd %s && ./teracli disable %s' % (const.teracli_dir, tablename) )
        wait_table_ready(tablename)
        runcmd('cd %s && ./teracli show %s' % (const.teracli_dir, tablename) )
        runcmd('cd %s && ./teracli drop %s' % (const.teracli_dir, tablename) )
        time.sleep(5)

def cleanup():
    """
    cleanup
    """
    drop_table("test")
    files = os.listdir('.')
    for f in files:
        if f.endswith('.out'):
            os.remove(f)
            
def print_debug_msg(sid=0, msg=""):
    """
    provide general print interface
    """
    print "@%d======================%s" % (sid, msg)

def execute_and_check_returncode(cmd, code):
    print(cmd)
    ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    ret.communicate()
    nose.tools.assert_equal(ret.returncode, code)

def exe_and_check_res(cmd):
    """
    execute cmd and check result
    """

    print cmd
    ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    nose.tools.assert_equal(ret.stderr.readlines(), [])


def clear_env():
    """
    clear env
    """

    print_debug_msg(4, "delete table_test001 and table_test002, clear env")
    drop_table("table_test001")
    drop_table("table_test002")


def cluster_op(op):
    if op == 'kill':
        print 'kill cluster'
        ret = subprocess.Popen(const.kill_script, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        print ''.join(ret.stdout.readlines())
        print ''.join(ret.stderr.readlines())
    elif op == 'launch':
        print 'launch cluster'
        ret = subprocess.Popen(const.launch_script, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        print ''.join(ret.stdout.readlines())
        print ''.join(ret.stderr.readlines())
    elif op == 'launch_ts_first':
        print 'launch cluster'
        ret = subprocess.Popen(const.launch_ts_first_script, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        print ''.join(ret.stdout.readlines())
    else:
        print 'unknown argument'
        nose.tools.assert_true(False)


def create_kv_table():
    print 'create kv table'
    cleanup()
    ret = subprocess.Popen(const.teracli_binary + ' create test', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    print ''.join(ret.stdout.readlines())
    print ''.join(ret.stderr.readlines())


def create_singleversion_table():
    print 'create single version table'
    cleanup()
    ret = subprocess.Popen(const.teracli_binary + ' create "test{cf0, cf1}"',
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    print ''.join(ret.stdout.readlines())
    print ''.join(ret.stderr.readlines())


def create_multiversion_table():
    print 'create multi version table'
    cleanup()
    ret = subprocess.Popen(const.teracli_binary + ' create "test{cf0<maxversions=20>, cf1<maxversions=20>}"',
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    print ''.join(ret.stdout.readlines())
    print ''.join(ret.stderr.readlines())


def createbyfile(schema, deli=''):
    """
    This function creates a table according to a specified schema
    :param schema: schema file path
    :param deli: deli file path
    :return: None
    """

    cleanup()
    create_cmd = '{teracli} createbyfile {schema} {deli}'.format(teracli=const.teracli_binary, schema=schema, deli=deli)
    print create_cmd
    ret = subprocess.Popen(create_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    print ''.join(ret.stdout.readlines())
    print ''.join(ret.stderr.readlines())


def rowread_table(table_name, file_path):
    allv = 'scan'
    
    tmpfile = 'tmp.file'
    scan_cmd = '{teracli} {op} {table_name} "" "" > {out}'.format(
        teracli=const.teracli_binary, op=allv, table_name=table_name, out=tmpfile)
    print scan_cmd
    ret = subprocess.Popen(scan_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    ret.communicate()

    tmpfile2 = 'tmp.file2'
    awk_args = ''
    awk_args += """-F ':' '{print $1}'"""
    awk_cmd = 'awk {args} {out} |sort -u > {out1}'.format(
            args=awk_args, out=tmpfile, out1=tmpfile2)
    print awk_cmd
    ret = subprocess.Popen(awk_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    ret.communicate()
    
    rowread_cmd = 'while read line; do {teracli} get {table_name} $line; done < {out1} > {output}'.format(
            teracli=const.teracli_binary, table_name=table_name, out1=tmpfile2, output=file_path)
    ret = subprocess.Popen(rowread_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    ret.communicate()

    #ret = subprocess.Popen('rm -rf tmp.file tmp.file2', stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    #ret.communicate()


def run_tera_mark(file_path, op, table_name, random, value_size, num, key_size, cf='', key_seed=1, value_seed=1):
    """
    This function provide means to write data into Tera and dump a copy into a specified file at the same time.
    :param file_path: a copy of data will be dumped into file_path for future use
    :param op: ['w' | 'd'], 'w' indicates write and 'd' indicates delete
    :param table_name: table name
    :param random: ['random' | 'seq']
    :param value_size: value size in Bytes
    :param num: entry number
    :param key_size: key size in Bytes
    :param cf: cf list, e.g. 'cf0:qual,cf1:flag'. Empty cf list for kv mode. Notice: no space in between
    :param key_seed: seed for random key generator
    :param value_seed: seed for random value generator
    :return: None
    """

    # write data into Tera
    tera_bench_args = ""
    awk_args = ""

    if cf == '':  # kv mode
        tera_bench_args += """--compression_ratio=1 --key_seed={kseed} --value_seed={vseed} """\
                           """ --value_size={vsize} --num={num} --benchmarks={random} """\
                           """ --key_size={ksize} """.format(kseed=key_seed, vseed=value_seed,
                                                             vsize=value_size, num=num, random=random, ksize=key_size)
        if op == 'd':  # delete
            awk_args += """-F '\t' '{print $1}'"""
        else:  # write
            awk_args += """-F '\t' '{print $1"\t"$2}'"""
    else:  # table
        tera_bench_args += """--cf={cf} --compression_ratio=1 --key_seed={kseed} --value_seed={vseed} """\
                           """ --value_size={vsize} --num={num} --benchmarks={random} """\
                           """ --key_size={ksize} """.format(cf=cf, kseed=key_seed, vseed=value_seed,
                                                             vsize=value_size, num=num, random=random, ksize=key_size)
        if op == 'd':  # delete
            awk_args += """-F '\t' '{print $1"\t"$3"\t"$4}'"""
        else:  # write
            awk_args += """-F '\t' '{print $1"\t"$2"\t"$3"\t"$4}'"""

    tera_mark_args = """--mode={op} --tablename={table_name} --type=async """\
                     """ --verify=false""".format(op=op, table_name=table_name)

    cmd = '{tera_bench} {bench_args} | awk {awk_args} | {tera_mark} {mark_args}'.format(
        tera_bench=const.tera_bench_binary, bench_args=tera_bench_args, awk_args=awk_args,
        tera_mark=const.tera_mark_binary, mark_args=tera_mark_args)

    runcmd(cmd)

    # write/append data to a file for comparison
    for path, is_append in file_path:
        if cf == '':
            awk_args = """-F '\t' '{print $1"::0:"$2}'"""
        else:
            awk_args = """-F '\t' '{print $1":"$3":"$4":"$2}'"""

        redirect_op = ''
        if is_append is True:
            redirect_op += '>>'
        else:
            redirect_op += '>'

        dump_cmd = '{tera_bench} {tera_bench_args} | awk {awk_args} {redirect_op} {out}'.format(
            tera_bench=const.tera_bench_binary, tera_bench_args=tera_bench_args,
            redirect_op=redirect_op, awk_args=awk_args, out=path)
	runcmd(dump_cmd)


def scan_table(table_name, file_path, allversion, snapshot=0, is_async=False):
    """
    This function scans the table and write the output into file_path
    :param table_name: table name
    :param file_path: write scan output into file_path
    :param allversion: [True | False]
    :param is_async: True for batch scan
    """

    allv = ''
    if allversion is True:
        allv += 'scanallv'
    else:
        allv += 'scan'
    
    if is_async is True:
        async_flag = '--tera_sdk_scan_async_enabled=true --v=30 --tera_client_scan_async_enabled=true'
    else:
        async_flag = '--tera_sdk_scan_async_enabled=false'
        
    snapshot_args = ''
    if snapshot != 0:
        snapshot_args += '--snapshot={snapshot}'.format(snapshot=snapshot)
    
    scan_cmd = '{teracli} {flags} {op} {table_name} "" "" {snapshot} > {out}'.format(
        teracli=const.teracli_binary, flags=async_flag, op=allv, table_name=table_name, snapshot=snapshot_args, out=file_path)
    runcmd(scan_cmd)


def get_tablet_list(table_name):
    # TODO: need a more elegant & general way to obtain tablet info
    show_cmd = '{teracli} show {table}'.format(teracli=const.teracli_binary, table=table_name)
    print show_cmd
    ret = subprocess.Popen(show_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    tablet_info = ret.stdout.readlines()[5:]  # tablet info starts from the 6th line
    tablet_info = filter(lambda x: x != '\n', tablet_info)
    tablet_paths = []
    for tablet in tablet_info:
        comp = filter(None, tablet.split(' '))
        tablet_paths.append(comp[2])
    return tablet_paths


def parse_showinfo():
    '''
    if you want to get show info, you can call this function to return with a dict
    '''
    show_cmd = '{teracli} show'.format(teracli=const.teracli_binary)
    print show_cmd
    ret = subprocess.Popen(show_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    table_info = ret.stdout.readlines()[2:-1]
    retinfo = {}
    for line in table_info:
        line = line.strip("\n")
        line_list = line.split(" ")
        list_ret = [line_list[i] for i in range(len(line_list)) if line_list[i] != ""]

        retinfo[list_ret[1]] = {}
        retinfo[list_ret[1]]["status"] = list_ret[2]
        retinfo[list_ret[1]]["size"] = list_ret[3]
        retinfo[list_ret[1]]["lg_size"] = [list_ret[j] for j in range(4, len(list_ret) - 2)]
        retinfo[list_ret[1]]["tablet"] = list_ret[len(list_ret) - 2]
        retinfo[list_ret[1]]["busy"] = list_ret[len(list_ret) - 1]

    print json.dumps(retinfo)
    return retinfo


def compact_tablets(tablet_list):
    # TODO: compact may timeout
    for tablet in tablet_list:
        compact_cmd = '{teracli} tablet compact {tablet}'.format(teracli=const.teracli_binary, tablet=tablet)
        print compact_cmd
        ret = subprocess.Popen(compact_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        print ''.join(ret.stdout.readlines())
        print ''.join(ret.stderr.readlines())


def snapshot_op(table_name):
    """
    This function creates | deletes a snapshot
    :param table_name: table name
    :return: snapshot id on success, None otherwise
    """
    # TODO: delete snapshot
    snapshot_cmd = '{teracli} snapshot {table_name} create'.format(teracli=const.teracli_binary, table_name=table_name)
    print snapshot_cmd
    ret = subprocess.Popen(snapshot_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out = ret.stdout.readlines()
    ret = ''
    try:
        ret += out[1]
    except IndexError:
        return None

    if ret.startswith('new snapshot: '):
        snapshot_id = ret[len('new snapshot: '):-1]
        if snapshot_id.isdigit():
            return int(snapshot_id)
    return None


def rollback_op(table_name, snapshot, rollback_name):
    """
    Invoke rollback action
    :param table_name: table name
    :param snapshot: rollback to a specific snapshot
    :return: None
    """
    rollback_cmd = '{teracli} snapshot {table_name} rollback --snapshot={snapshot} --rollback_name={rname}'.\
        format(teracli=const.teracli_binary, table_name=table_name, snapshot=snapshot, rname=rollback_name)
    print rollback_cmd
    ret = subprocess.Popen(rollback_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    print ''.join(ret.stdout.readlines())


def compare_files(file1, file2, need_sort):
    """
    This function compares two files.
    :param file1: file path to the first file
    :param file2: file path to the second file
    :param need_sort: whether the files need to be sorted
    :return: True if the files are the same, False on the other hand
    """
    if need_sort is True:
        sort_cmd = 'sort {f1} > {f1}.sort; sort {f2} > {f2}.sort'.format(f1=file1, f2=file2)
        print sort_cmd
        ret = subprocess.Popen(sort_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        print ''.join(ret.stdout.readlines())
        print ''.join(ret.stderr.readlines())
        os.rename(file1+'.sort', file1)
        os.rename(file2+'.sort', file2)
    return filecmp.cmp(file1, file2, shallow=False)


def file_is_empty(file_path):
    """
    This function test whether a file is empty
    :param file_path: file path
    :return: True if the file is empty, False on the other hand
    """
    return not os.path.getsize(file_path)


def cleanup_files(file_list):
    for file_path in file_list:
        os.remove(file_path)

def check_show_user_result(cmd, should_contain, substr):
    print(cmd)
    ret = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdoutdata = ''.join(ret.stdout.readlines())
    if should_contain:
        nose.tools.assert_true(substr in stdoutdata)
    else:
        nose.tools.assert_true(substr not in stdoutdata)
