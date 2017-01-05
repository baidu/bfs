from BfsSdk import BfsSdkException
from BfsSdk import FS
import time

if __name__ == '__main__':
    try:
        fs = FS()
        '''
        fs.ListDirectory("/")
        fs.Rmdir("/", True)
        fs.ListDirectory("/")


        fs.CreateDirectory("/test")

        print "touch", fs.Touchz(["file0", "file1", "file2"])
        time.sleep(1)

        fs.ListDirectory("/")
        time.sleep(1)

        print "delete", fs.DeleteFile(["file0", "file"])
        time.sleep(1)

        fs.Rename("test", "test2")
        time.sleep(1)

        fs.ListDirectory("/")
        time.sleep(1)

        fs.Symlink("file1", "link")
        time.sleep(1)

        fs.Put("/home/users/sunjinjin01/bfs/sandbox/bfs.flag", "/")
        time.sleep(1)

        fs.ListDirectory("/")
        time.sleep(1)

        print fs.Cat(["bfs.flag"])
        time.sleep(1)

        fs.Get("/bfs.flag", "/home/users/sunjinjin01/")
        time.sleep(1)

        fs.Du(["/bfs.flag"])
        time.sleep(1)

        fs.Chmod("777", "/bfs.flag")
        time.sleep(1)

        fs.Location("/bfs.flag")
        time.sleep(1)
'''
        fs.ChangeReplicaNum("/bfs.flag", "4")
        fs.Location("/bfs.flag")

    except BfsSdkException as e:
        print(e.reason)







