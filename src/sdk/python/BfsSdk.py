# -*- coding: utf-8 -*-

'''
Bfs Python SDK. it needs a libbfs_c.so
'''
from ctypes import CFUNCTYPE, POINTER
from ctypes import byref, cdll, string_at
from ctypes import c_bool, c_char_p, c_void_p
from ctypes import c_uint32, c_int32, c_int64, c_ubyte, c_uint64
lib = cdll.LoadLibrary('/home/users/sunjinjin01/bfs/libbfs_c.so')

class BfsSdkException(Exception):
    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return self.reason


class Status(object):
    OK = 0
    BAD_PARAMETER = 1
    PERMISSION_DENIED = 2
    NOT_ENOUGH_QUOTA = 3
    NETWORK_UNAVAILABLE = 4
    TIMEOUT = 5
    NOT_ENOUGH_SPACE = 6
    OVERLOAD = 7
    META_NOT_AVAILABLE = 8
    UNKNOWN_ERROR = 9
    reason_list_ = ["ok",  "bad parameter", "permission denied"
    		"not enough quota", "network unavaliable", "timeout",
    		"not enough space", "overload", "meta not available",
    		"unkown error"]

    def __init__(self, c):
        self.c_ = c
        if c < 0 or c > len(Status.reason_list_)-1:
            self.reason_ = "bad status code"
        else:
            self.reason_ = Status.reason_list_[c]

    def GetReasonString(self):
        return Status.reason_list_[self.c_]

    def GetReasonNumber(self):
        return self.c_


class FS(object):
    def __init__ (self):
        self.fs = lib.bfs_open_file_system()
        if self.fs is None:
        	raise BfsSdkException("open file system failed")

    def  CreateDirectory(self, path):
        result = lib.bfs_create_directory(self.fs, path)
        if not result:
        	raise BfsSdkException("mkdir: %s failed, exception: %s" % path % Status(result).GetReasonString())

if __name__ == '__main__':
    print ('test')
    try:
        fs = FS()
        fs.CreateDirectory("/test")
    except BfsSdkException as e:
        print(e.reason)







