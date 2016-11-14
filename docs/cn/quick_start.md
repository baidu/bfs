快速上手
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
   待完善...
   
## 从入门到精通
### 为什么使用BFS
传统的分布式文件系统(如HDFS)Namenode是一个单点，容量和吞吐量都不以扩展，同时使用外部的日志同步工具（如QJM、ZooKeeper）failover时间是分钟级的。  
BFS主要从扩展性和持续可用两个方面做了改进，采用分布式的NameServer，多NameServer之间使用内置的Raft协议做日志同步，保证failover时，新主一定有最新的状态不需要额外日志同步，解决了扩展性和可用性问题。  
### BFS部署
#### 依赖
BFS使用了C++实现，所以部署不依赖Java虚拟机。  
BFS内置一致性协议选主，所以部署不依赖外部协调模块。  
总之，BFS的部署不依赖外部系统，但你需要有一个主流的Linux发行版：RedHat、CentOS或者Ubuntu，并有Gcc4.8及以上版本编译器。  
#### 配置
一个典型的bfs配置文件:  
\# 公共配置  
--bfs_log=../log/bfs.log   # log文件的存储路径  
--bfs_log_size=5120        # 单个log文件的大小  
--bfs_log_limit=1024000    # 总的log限制,超过限制会删除旧的log  
--bfs_log_level=4          # 日志级别 DEBUG = 2, INFO = 4, WARNING = 8, FATAL = 16,  
--nameserver_nodes=ns1.baidu.com:8828,ns2.baidu.com:8828,ns3.baidu.com:8828   # NameServer节点列表  
  
\#NameServer配置  
--ha_strategy=raft         # NameServer的HA策略, none,raft,master-slave 3选一  
--node_index=0             # 当前节点是NameServer节点列表的第几个  
--keepalive_timeout=30     # ChunkServer心跳超时时间  
--namedb_path=/ssd1/meta   # NameServer的meta信息存储路径  
--default_replica_num=3    # 默认文件副本数  
  
\#ChunkServer配置  
--chunkserver_max_unfinished_byte=2147483648 # 未完成的写请求会占用ChunkServer内存,为避免OOM,设置了一个上限超出后就拒绝新请求.  
--block_store_path=/home/disk1,/home/disk2   # 数据存储目录, 每个磁盘指定一个  
  
\#SDK配置
--sdk_file_reada_len=10485760                # 文件预读最大长度，当判断用户在顺序读文件时，会自动开启预读。  

#### 分布式部署
BFS的分布式部署一般包含3或5个NameServer节点和3+个ChunkServer节点。  
1. 在每个bfs.flag里通过`nameserver_nodes`指定所有NameServer的主机名和端口  
2. 在NameServer的bfs.flag里通过`node_index`指定NameServer对应的序号  
3. 以任意顺序或并发`启动3个NameServer`  
4. 在ChunkServer的bfs.flag里通过block_store_path指定ChunkServer使用的`磁盘路径`，并确保chunkserver对这个路径拥有`写权限`。  
5. 以任意顺序或并发`启动所有的chunkserver`  
6. 通过`./bfs_client stat检查集群状态`，如果看到所有的chunkserver存活，即搭建完成  

### BFS使用
使用Demo参考[src/client/bfs_client.cc](https://github.com/baidu/bfs/blob/master/src/client/bfs_client.cc)
