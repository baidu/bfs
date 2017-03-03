[The Baidu File System](http://github.com/baidu/bfs)
=======

 [![Build Status](https://travis-ci.org/baidu/bfs.svg?branch=master)](https://travis-ci.org/baidu/bfs)  [![Build Status](https://scan.coverity.com/projects/8135/badge.svg)](https://scan.coverity.com/projects/myawan-bfs-1/) 

The Baidu File System (BFS) is a distributed file system designed to support real-time applications. Like many other distributed file systems, BFS is highly fault-tolerant. But different from others, BFS provides low read/write latency while maintaining high throughput rates. Together with [Galaxy](https://github.com/baidu/galaxy) and [Tera](http://github.com/baidu/tera), BFS supports many real-time products in Baidu, including Baidu webpage database, Baidu incremental indexing system, Baidu user behavior analysis system, etc.

## Features
1. Continuous availability 
	* Nameserver is implemented as a `raft group`, no single point failure.
2. High throughput
	* High performance data engine to maximize IO utils.
3. Low latency
	* Global load balance and slow node detection.
4. Linear scalability
	* Support multi data center deployment and up to 10,000 data nodes.

## Architecture
![架构图](resources/images/bfs-arch2-mini.png)

## Quick Start
#### Build  
    ./build.sh
#### Standalone BFS
    cd sandbox
    ./deploy.sh
    ./start_bfs.sh

## How to Contribute
1. Please read the [RoadMap](docs/en/roadmap.md) or source code.  
2. Find something you are interested in and start working on it.
3. Test your code by simply running `make test` and `make check`.
4. Make a pull request.
5. Once your code has passed the code-review and merged, it will be run on thousands of servers :)


## Contact us
opensearch@baidu.com

====

[百度文件系统](http://github.com/baidu/bfs)
====

百度的核心业务和数据库系统都依赖分布式文件系统作为底层存储，文件系统的可用性和性能对上层搜索业务的稳定性与效果有着至关重要的影响。现有的分布式文件系统（如HDFS等）是为离线批处理设计的，无法在保证高吞吐的情况下做到低延迟和持续可用，所以我们从搜索的业务特点出发，设计了百度文件系统。

## 核心特点
1. 持续可用  
	* 数据多机房、多地域冗余，元数据通过Raft维护一致性，单个机房宕机，不影响整体可用性。  
2. 高吞吐  
	* 通过高性能的单机引擎，最大化存储介质IO吞吐；  
3. 低延时  
	* 全局负载均衡、慢节点自动规避  
4. 水平扩展  
	* 设计支持两地三机房，1万+台机器管理。  

## 架构
![架构图](resources/images/bfs-arch2-mini.png)

## 快速试用
#### 构建
    ./build.sh
#### 单机版BFS
    cd sandbox
    ./deploy.sh
    ./start_bfs.sh

## 如何参与开发
1. 阅读[RoadMap](docs/cn/roadmap.md)文件或者源代码，了解我们当前的开发方向
2. 找到自己感兴趣开发的的功能或模块
3. 进行开发，开发完成后自测功能是否正确，并运行make test及make check检查是否可以通过已有的测试case
4. 发起pull request
5. 在code-review通过后，你的代码便有机会运行在百度的数万台服务器上~


## 联系我们
邮件：opensearch@baidu.com  
QQ群：188471131

