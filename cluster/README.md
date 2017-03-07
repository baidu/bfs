# Cluster
One shell script deploy and manage cluster

## Depend
Server which is running deploy-all.sh should be able to login other server using ssh whithout password

## Cluster.ini
Cluster config file, there is two example config file named cluser_none.ini and cluster_masterslave.ini
Format:
Strategy section is describe the cluster's HA strategy, only support master_slave and none now.
Server section is all server name in cluster, `servers = ns0 ns1 cs0 cs1 cs2 cs3` means cluster have six servers named ns0 ns1 cs0 cs1 cs2 cs3.
Every server has a Section which section has ip, port, username, role, serverpath, index.
Ip is server's ip, post is the port server will use, username will be using  fot ssh login, role is nameserver/chunkserver, serverpath is server's dirctory, index only will be using in nameserver
user can use cluser_none.ini or cluster_masterslave.ini for default local test.

## Deployment
`bash deploy-all.sh`
This script will deploy all cluster enviroment
`bash start-all.sh`
This script will start all server's program
`bash stop-all.sh`
This script will stop all server's program

## Launch
`bash deploy-all.sh
bash start-all.sh
`
This script will deploy namserver and chunkserver enviroment and start service

## Testing
To create a directory:
`./bfs_client mkdir /user`

Put a binary to our cluster:
`./bfs_client put chunkserver /chunkserver`

List a directory:
`./bfs_client ls /`

Download a file to local fs:
`./bfs_client get /chunkserver ./file_from_bfs`

## Cleanup
To tear down the cluster and cleanup the environment:
`bash clear-all.sh`

# 集群
BFS的集群部署脚本

## 依赖
部署脚本所在的服务器应该能够免密码登录其他的服务器

## Cluster.ini
集群配置文件，有两个集群配置文件模板cluser_none.ini和cluster_masterslave.ini。
格式:
strategy这一节只有一个配置项strategy，描述集群采用的ha方式, 目前只支持none和master_slave模式
server这一节只有servers这个配置项，描述的是集群所有服务器的名称
每一个服务器的名称都会作为一节的名称，该节的内容是ip, port, username, role,serverpath, index
ip和port都是该服务运行在的服务器ip, port, username是ssh登录使用的用户名，role代表该服务是chunkserver还是nameserver, serverpath代表在此服务器上的启动程序所在的目录，index只有在nameserver的master_slave的HA模式下才有，代表nameserver的编号

## 部署
`bash deploy-all.sh`
一键部署集群环境
`bash start-all.sh`
启动每一个服务器的服务
`bash stop-all.sh`
停止所有服务器的服务

## 启动
`bash start-all.sh`
启动集群服务

## 测试
创建目录:
`./bfs_client mkdir /user`

把文件放入远端集群:
`./bfs_client put chunkserver /chunkserver`

查看目录下的文件:
`./bfs_client ls /`

下载一个文件:
`./bfs_client get /chunkserver ./file_from_bfs`

## 清理
停止所有服务并清理集群环境
`bash clear-all.sh`

## 集群配置文件
集群所有服务器器所在的ip以及
