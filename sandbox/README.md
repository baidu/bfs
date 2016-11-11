# Sandbox
A toy environment for testing.

## Deployment
`bash deploy.sh`
This script will prepare the environment to run Nameserver and Chunkservers.

## Launch
`bash deploy.sh`
This script will launch Nameserver and Chunkservers to put up a BFS cluster(only it is not a real cluster, but a simulation on one server). Logs will be saved under nameserver0/log and chunkserver[id]/log.

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
`bash clear.sh`

# Sandbox
用于DFS的单机部署测试。
## 部署
`bash deploy.sh  `
会创建Nameserver和Chunkserver运行环境

## 启动
`bash start_bfs.sh  `
启动Nameserver与多个Chunkserver，形成一个单机（模拟）集群。
日志会被打印在nameserver0/log和chunkserver[id]/log

## 测试
创建文件夹：  
`./bfs_client mkdir /user`

将chunkserver的二进制作为一个普通文件上传到集群上：  
`./bfs_client put chunkserver /chunkserver`

查看文件：  
`./bfs_client ls /`

将文件下载到本地  
`./bfs_client get /chunkserver ./file_from_bfs`

## 清理
`bash clear.sh`


