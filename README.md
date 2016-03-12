[The Baidu File System](http://github.com/bluebore/dfs)
======

Travis [![Build Status](https://travis-ci.org/bluebore/bfs.svg)](https://travis-ci.org/bluebore/bfs) Jenkins [![Build Status](http://220.181.7.231/buildStatus/icon?job=bfs_master)](http://220.181.7.231/view/bfs/job/bfs_master/) Coverity [![Build Status](https://scan.coverity.com/projects/8135/badge.svg)](https://scan.coverity.com/projects/myawan-bfs-1/)

##背景
百度的核心数据库[Tera](http://github.com/baidu/tera)将数据持久化在分布式文件系统上，分布式文件系统的性能、可用性和扩展性对整个上层搜索业务的稳定性与效果有着至关重要的影响。现有的分布式文件系统无法很好地满足这几方面的要求，所以我们从Tera需求出发，开发了百度自己的分布式文件系统。

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

## 日志规则与说明
为了简化日志打印，并便于grep,  
所有block id的打印使用“#%ld "的格式（即前加#，后加空格）  
所有chunkserver id打印使用"C%d "的格式  
所有entry id打印使用"E%ld "的格式  
所有block version打印使用"V%ld "的格式

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
yangce~
