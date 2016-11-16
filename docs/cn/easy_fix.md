# Easy Fix
这里记录一些待修复，同时初步判断比较容易修复的问题，未选定的是还没有人去做的。

如果你发现某一项实际已经被完成了,但这里未选定,那么你可以直接提交一个pr选定它。

### 文档类
- [ ] 首先就是本文自己的错别字,需要review

### 代码类
- [ ] [fuse代码](https://github.com/baidu/bfs/blob/master/fuse/bfs_mount.cc) review,找到其中拼写或明显的错误

### 环境类
- [ ] 非bash执行small_test.sh时出错, 98: small_test.sh: Syntax error: Bad for loop variable
- [ ] 合并sandbox下的deploy.sh和start_bfs.sh,或者加一个stop_bfs.sh
- [ ] Mac下编译通过
- [ ] Mac下运行
- [ ] Ubuntu下编译通过
- [ ] Ubuntu下运行
- [ ] Windows下Cygwin编译通过
