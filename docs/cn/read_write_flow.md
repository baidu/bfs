# BFS读写流程

## BFS文件Create流程
1. sdk调用`NameServer::CreateFile`, 向NameServer请求创建文件
2. NameServer检查合法性, 分配entryid, 写入Namespace

## BFS文件Write流程
1. sdk调用`NameServer::AddBlock`,向NameServer申请文件block, NameServer根据集群chunkserver负载, 为新block选择几个chunkserver
2. sdk调用`ChunkServer::WriteBlock`, 向每个chunkser(扇出写)或第一个chunkserver(链式写)发送写请求

## `ChunkServer::WriteBlock`流程
1. 如果是链式写, 当前不是复制链上最后一个cs, 异步调用下一个cs的`ChunkServer::WriteBlock`, 并通过回调触发下一步
2. 如果是当前块的第一个写操作, 调用BlockManager::CreateBlock, 创建一个新的Block, 并确定放在哪块Disk上
3. 将写buffer数据复制一份, 放入滑动窗口
4. 由滑动窗口的回调触发, 将buffer数据放入对应Disk的写队列
5. Disk类的后台写线程, 负责将写队列的buffer数据, 按顺序写入磁盘文件
6. 如果是当前块的最后一个请求, 通过`ChunkServer::BlockReceived`, 通知NameServer当前block副本完成

## BFS文件Sync流程
1. 文件创建后的第一次Sync, sdk等待所有后台的WriteBlock操作完成后, 调用`NameServer::SyncBlock` 向Nameserver报告文件大小
2. 从第二次Sync开始, sdk只等待所有后台的WriteBlock完成 不向NameServer通知

## BFS文件Close流程
1. 文件的最后一个`ChunkServer::WriteBlock`请求会附带标记, 通知Chunkserver关闭Block
2. 执行文件Sync逻辑保证说有数据都写到chunkserver
3. SDK调用`NameServer::FinishBlock`, 通知NameServer文件关闭, 告知文件实际大小

## BFS文件Read流程
1. sdk调用`GetFileLocation`,获得文件所在chunkserver
2. sdk调用`ReadBlock`, 从chunkserver上读取文件数据

## BFS文件Delete流程
1. sdk调用`NameServer::Unlink`, 请求NameServer删除文件
2. NameServer检查合法性, 从NameSpace里删除对应entry
3. 在Chunkserver做"BlockReport"时, NameServer在response里告知对应的Chunkserver做block的物理删除
