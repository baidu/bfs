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
4. 体验BFS  
   可以使用bfs_client与集群交互，直接输入`./bfs_client`可以看到提示，  
   这里举几个例子：  
   list： ./bfs_client ls /  
   touchz: ./bfs_client touchz /empty_file  
   put: ./bfs_client put bfs_client /  
   get: ./bfs_client get /bfs_client ./bfs_client.bak  
   
## 从入门到精通
BFS使用了C++11,所以你应该有个gcc4.8以上的编译器  
BFS依赖了几个库  
protobuf
sofa-pbrpc

