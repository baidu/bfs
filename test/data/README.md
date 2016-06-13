# Sandbox
用于DFS的单机部署测试。
## 部署
sh deploy.sh  
会创建Nameserver和Chunkserver运行环境

## 启动
sh start_bfs.sh  
启动Nameserver与多个Chunkserver，形成一个单机（模拟）集群。
日志会被打印在nameserver/nlog和chunkserverN/clogN

## 测试
创建文件夹：  
./bfs_client mkdir /user

将chunkserver的二进制作为一个普通文件上传到集群上：  
./bfs_client put chunkserver /chunkserver

查看文件：  
./bfs_client ls /

将文件下载到本地  
./bfs_client get /chunkserver ./file_from_bfs

## 清理
sh clear.sh
