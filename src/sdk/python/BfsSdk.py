# -*- coding: utf-8 -*-

'''
Bfs Python SDK. it needs a libbfs_c.so
'''
from ctypes import CFUNCTYPE, POINTER
from ctypes import byref, cdll, string_at
from ctypes import c_bool, c_char_p, c_void_p
from ctypes import c_uint32, c_int32, c_int64, c_ubyte, c_uint64
lib = cdll.LoadLibrary('./libbfs_c.so')

class BfsSdkException(Exception):
    def __init__(self, reason):
        self.reason = reason
    def __str__(self):
        return self.reason

class Status(object):
    # bfs.h: const error code
    OK = 0
    BAD_PARAMETER = -1
    PERMISSION_DENIED = -2
    NOT_ENOUGH_QUOTA = -3
    NETWORK_UNAVAILABLE = -4
    TIMEOUT = -5
    NOT_ENOUGH_SPACE = -6
    OVERLOAD = -7
    META_NOT_AVAILABLE = -8
    UNKNOWN_ERROR = -9
    reason_list_ = ["ok",  "bad parameter", "permission denied",
    		"not enough quota", "network unavaliable", "timeout",
    		"not enough space", "overload", "meta not available",
    		"unkown error"]

    def __init__(self, c):
        self.c_ = c
        if -c < 0 or -c > len(Status.reason_list_)-1:
            self.reason_ = "bad status code"
        else:
            self.reason_ = Status.reason_list_[-c]

    def GetReasonString(self):
        return self.reason_

    def GetReasonNumber(self):
        return self.c_


class FS(object):
    def __init__ (self):
        self.fs = lib.bfs_open_file_system()
        if self.fs is None:
        	raise BfsSdkException("open file system failed")

    def CreateDirectory(self, path):
        result = lib.bfs_create_directory(self.fs, path)
        if result != 0:
        	raise BfsSdkException("Create dir %s failed, exception: %s" % (path, Status(result).GetReasonString()))

    def Touchz(self, paths):
        failed = list()
        for path in paths:
            result = lib.bfs_touchz(self.fs, path)
            if result != 0:
                failed.append(path)
        return failed

    def ListDirectory(self, path):
        result = lib.bfs_list_directory(self.fs, path)
        if result != 0:
            raise BfsSdkException("List dir %s failed, exception: %s" % (path, Status(result).GetReasonString()))

    def DeleteFile(self, paths):
        failed = list()
        for path in paths:
            result = lib.bfs_delete_file(self.fs, path)
            if result != 0:
                failed.append(path)
        return failed

    def Rename(self, oldpath, newpath):
        result = lib.bfs_rename(self.fs, oldpath, newpath)
        if result != 0:
            raise BfsSdkException("Rename %s to %s failed, exception: %s" % (oldpath, newpath, Status(result).GetReasonString()))

    def Symlink(self, src, dst):
        result = lib.bfs_symlink(self.fs, src, dst)
        if result != 0:
            raise BfsSdkException("Create Symlink %s to %s failed, exception: %s" % (oldpath, newpath, Status(result).GetReasonString()))

    def Cat(self, paths):
        failed = list()
        for path in paths:
            result = lib.bfs_cat(self.fs, path)
            if result != 0:
                failed.append(path)
        return failed

    def Get(self, bfs, local):
        # copy bfs to local
        result = lib.bfs_get(self.fs, bfs, local)
        exception = {
            1: "Bad source %s" % bfs,
            2: "Open bfs file fail %s" % bfs,
            3: "Open local file fail %s" % local,
            4: "Read source fail %s" % bfs
        }
        if result != 0:
            raise BfsSdkException("Get from %s to %s failed, exception: %s" % (src, tgt, exception[result]))

    def Put(self, local, bfs):
        #copy local to bfs
        result = lib.bfs_put(self.fs, local, bfs)
        exception = {
            1: "Bad source %s" % local,
            2: "Open local file fail %s " % local,
            3: "Get local file stat fail %s " % local,
            4: "Open bfs file fail %s" % bfs,
            5: "Write bfs file fail %s" % bfs,
            6: "Close bfs file fail %s" % bfs
        }
        if result != 0:
            raise BfsSdkException("Put from %s to %s failed, exception: %s" % (local, bfs, exception[result]))

    def Du(self, paths):
        failed = list()
        for path in paths:
            result = lib.bfs_du(self.fs, path)
            if result != 0:
                failed.append(path)
        return failed

    def Rmdir(self, paths, recursive):
        failed = list()
        for path in paths:
            result = lib.bfs_rm_dir(self.fs, path, recursive)
            if result != 0:
                failed.append(path)
        return failed

    def ChangeReplicaNum(self, path, replica_num):
        result = lib.bfs_change_replica_num(self.fs, path, replica_num)
        if result != 0:
            raise BfsSdkException("ChangeReplicaNum %s failed, exception: %s" % (path, Status(result).GetReasonString()))

    def Chmod(self, mode, path):
        result = lib.bfs_chmod(self.fs, mode, path)
        if result != 0:
            raise BfsSdkException("Chmod mode %s file %s failed, exception: %s" % (mode, path, Status(result).GetReasonString()))

    def Location(self, path):
        result = lib.bfs_location(self.fs, path)
        if result != 0:
            raise BfsSdkException("GetFileLocation  %s failed, exception: %s" % (path, Status(result).GetReasonString()))

