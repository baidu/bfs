#介绍
这是一个bfs的集群管理工具，能够进行集群化的部署，启动，关闭等操作。

#serverfile介绍
集群化的部署需要一个server的文件，文件的书写要有一定的格式，不然会有不可预知的错误。
第一行是集群的高可用方案，现在暂时不支持raft模式。格式是strategy none/master_slave表示高可用的方案是master_slave还是none的
后续就是服务器信息，格式是
server <servername> <ip> <port> <hostname> <username> <password> <role> <bfslocation> [option]
server是这一行的标识，servername是用户确定的服务名称，ip和port是服务器所在ip和端口，hostname是主机名，username和password是ssh登录的账户和密码，role是服务的角色，是nameserver还是chunkserver，bfslocation是启动文件所在的目录，[option]是可选参数，目前只有master_slave模式的时候后边带有master和slave的标识。
举个none模式的serverfile例子
strategy none
server ns0 127.0.0.1 8020   hostname username passwd nameserver  /home/ubuntu/github/nameserver0
server cs0 127.0.0.1 8027   hostname username passwd chunkserver /home/ubuntu/github/chunkserver0
server cs1 127.0.0.1 8028   hostname username passwd chunkserver /home/ubuntu/github/chunkserver1
server cs2 127.0.0.1 8029   hostname username passwd chunkserver /home/ubuntu/github/chunkserver2
server cs3 127.0.0.1 8030   hostname username passwd chunkserver /home/ubuntu/github/chunkserver3
举个master_slave模式的serverfile例子
strategy master_slave
server ns0 127.0.0.1 8020   hostname username passwd nameserver  /home/ubuntu/github/nameserver0 master
server ns1 127.0.0.1 8021   hostname username passwd nameserver  /home/ubuntu/github/nameserver1 slave
server cs0 127.0.0.1 8027   hostname username passwd chunkserver /home/ubuntu/github/chunkserver0
server cs1 127.0.0.1 8028   hostname username passwd chunkserver /home/ubuntu/github/chunkserver1
server cs2 127.0.0.1 8029   hostname username passwd chunkserver /home/ubuntu/github/chunkserver2
server cs3 127.0.0.1 8030   hostname username passwd chunkserver /home/ubuntu/github/chunkserver3

## 准备
在启动之前，需要安装sshpass工具，在debian系列的系统下执行apt-get install sshpass,如果是radhat的则执行yum install sshpass

## 部署
sh deploy.sh serverfile
会创建Nameserver和Chunkserver运行环境

## 启动
sh start-all.sh serverfile
启动Nameserver与多个Chunkserver，形成一个集群。日志会被打印在nameserver0/log和chunkserver[id]/log

## 测试
sh small_test.sh serverfile
进行小规模的自动化测试

## 清理
sh clear.sh serverfile


