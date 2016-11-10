百度文件系统之快速上手
======

快速也分两种，一种不求甚解的10分钟快速体验，一种是知其然又知其所以然。

## 10分钟快速体验
1. 构建BFS  
   `./build.sh`  
   build.sh会自动下载、编译所有依赖，并最后编译出nameserver、chunkserver、bfs_client、libbfs.a几个二进制。  
   注：这步如果发现什么问题，可以直接提`Issue`，当然我们也欢迎你自己解决的提`Pull request`。
2. 部署BFS  
   `cd sandbox;`  
   `./deploy.sh`  
   deploy.sh 会自动部署一套有1个nameserver,4个chunkserver的模拟集群环境。  
3. 启动BFS  
   `./start.sh`  
   start.sh会启动前面异步部署的模拟集群环境  
   可以通过 `./bfs_client stat` 查看集群后启动情况，也可以通过`http://localhost:8828` 查看web页面信息  
   看到4个活的chunkserver, 说明集群启动成功.  
4. 使用bfs_client体验BFS  
   可以使用bfs_client与集群交互，直接输入`./bfs_client`可以看到提示，  
   这里举几个例子：  
   list： ./bfs_client ls /  
   touchz: ./bfs_client touchz /empty_file  
   put: ./bfs_client put bfs_client /  
   get: ./bfs_client get /bfs_client ./bfs_client.bak  
5. 使用SDK访问BFS  
   
   
## 从入门到精通
### 为什么使用BFS
传统的分布式文件系统(如HDFS)Namenode是一个单点，容量和吞吐量都不以扩展，同时使用外部的日志同步工具（如QJM、ZooKeeper）failover时间是分钟级的。  
BFS主要从扩展性和持续可用两个方面做了改进，采用分布式的NameServer，多NameServer之间使用内置的Raft协议做日志同步，保证failover时，新主一定有最新的状态不需要额外日志同步，解决了扩展性和可用性问题。
### BFS部署和依赖
#### BFS的依赖
BFS使用了C++11实现，所以部署不依赖Java虚拟机。  
BFS内置一致性协议选主，所以部署不依赖外部协调模块。  
总之，BFS的部署不依赖外部系统，但你需要有一个主流的Linux发行版：RedHat、CentOS或者Ubuntu，并有Gcc4.8以上版本编译器。

