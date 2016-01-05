[The Baidu File System](http://github.com/bluebore/dfs)
======

Travis [![Build Status](https://travis-ci.org/bluebore/dfs.svg)](https://travis-ci.org/bluebore/dfs)

Jenkins [![Build Status](http://220.181.7.231/buildStatus/icon?job=bfs_master)](http://220.181.7.231/view/bfs/job/bfs_master/)

##背景
百度的核心数据库[Tera](http://github.com/baidu/tera)将数据持久化在分布式文件系统上，文件系统的性能、稳定性和扩展性意义重大，当前使用中的文件系统无法满足Tera在这几方面的需求，所以我们从Tera需求出发，开发了百度自己的分布式文件系统。

##前世
突然想写个分布式文件系统~
  1. 支持表格系统的持久化数据存储
  2. 支持混布系统的临时数据存储
  3. 支持mapreduce的大文件存储


想加入的人在这留个名吧：

yanshiguang~  
yuanyi~  
yuyangquan~  
leiliyuan~

## 构建
在百度内部，可以直接运行：  
sh internal_build.sh  
外部构建请参考.travis.yml中的步骤。  

## Sandbox
Sandbox目录下包含了运行单机测试的环境和脚本。  
deploy.sh： 在本地部署一个包含4个chunkserver、1个nameserver的集群  
start.sh: 启动部署好的集群  
clear.sh: 清理集群  
small_test.sh 简单的自动化测试脚本，会调用上面三个脚本，并使用bfs_client测试文件系统的基本功能

