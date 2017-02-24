# BFS读写流程

## BFS文件Create流程
1. sdk调用`NameServer::CreateFile`, 向NameServer请求创建文件
2. NameServer检查合法性, 分配entryid, 写入Namespace

## BFS文件Write流程
1. sdk调用`NameServer::AddBlock`,向NameServer申请文件block, NameServer根据集群chunkserver负载, 为新block选择几个chunkserver
2. sdk调用`ChunkServer::WriteBlock`, 向每个chunkser(扇出写)或第一个chunkserver(链式写)发送写请求

## BFS文件Sync流程
1. 文件创建后的第一次Sync, sdk等待所有后台的WriteBlock操作完成后, 调用`NameServer::SyncBlock` 向Nameserver报告文件大小
2. 从第二次Sync开始, sdk只等待所有后台的WriteBlock完成 不向NameServer通知

## BFS文件Close流程
1. 文件的最后一个`ChunkServer::WriteBlock`请求会附带标记, 通知Chunkserver关闭Block
2. Chunkserver通过`ChunkServer::BlockReceived`, 通知NameServer当前block副本完成
3. SDK调用`NameServer::FinishBlock`, 通知NameServer文件关闭, 告知文件实际大小

## BFS文件Read流程
1. sdk调用`GetFileLocation`,获得文件所在chunkserver
2. sdk调用`ReadBlock`, 从chunkserver上读取文件数据

## BFS文件Delete流程
1. sdk调用`NameServer::Unlink`, 请求NameServer删除文件
2. NameServer检查合法性, 从NameSpace里删除对应entry
3. 在Chunkserver做"BlockReport"时, NameServer在response里告知对应的Chunkserver做block的物理删除
