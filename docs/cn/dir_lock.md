# 目录锁背景

Tera中，`Master`通过`Nexus`判断`TabletNode`是否仍在提供服务，当`Master`认为某个`TabletNode`已无法提供服务后，会将其上面所有`Tablet`迁移到其它`TabletNode`上进行Load。然而，在发生网络分区，或者网络延时造成的Tera `Master`处理时序错乱时，可能造成旧的`TabletNode`上仍Load某个`Tablet`时，同时给另外一个新的`TabletNode`发送Load某个`Tablet`的命令，造成同一个`Tablet`被多台`TabletNode`同时Load，从而造成数据写入错乱。因此，必须依赖Tera使用的底层分布式文件系统来彻底解决多重Load问题：Tera中的每个`Tablet`的数据都存放在一个单独的目录下，在对`Tablet`进行Load时，持有对应目录的一个排它锁，便可以防止有其它`TabletNode`同时Load此`Tablet`

# 整体设计

1. 在`FileInfo`中添加`lock_status`以及`holder`标记，`lock_status`可能处于`locked`、`unlock`、`cleaning`三种状态，分别对应目录已上锁，未上锁，正在清锁三种状态，当状态为`locked`时，`holder`中记录的为持锁者的标识(`ip:port:timestamp`，此标识由客户端(sdk)在加锁时发送给`NameServer`，在客户端存活期间保持不变
2. 在对目录进行加锁时，首先检查此目录的`FileInfo`中`lock_status`是否为`unlock`:
   - 如果是，则将其状态更改为`locked`，并连同调用者的`ip:port`一同持久化到`namespace`中，加锁成功
   - 如果不是，则目录锁的状态为`locked`或`cleaning`，加锁失败，将锁目前的状态返回给调用者
3. 在对目录释放锁时，首先检查此目录的`FileInfo`中`lock_statu`是否为`locked`:
   - 如果是，将其状态改为`cleaning`并持久化到`namespace`中，然后，将此目录下所有正在写的文件及其所处在的`ChunkServer`地址均扫描出来，构造一个`CloseFilesCtx`结构，后台依次将其关闭，待全部文件均收到`BlockReceived`之后，可以认为全部文件已经被关闭，此时将`namespace`中对应目录的锁状态更改为`unlocked`，后续允许对此目录进行再次加锁
   - 如果不是，则此RPC为乱序或者重试的调用，可以不用处理，日志中将其记录即可
4. 在创建新文件或者删除文件时，先检查其父目录的`FileInfo`中锁的状态:
   - 如果为`unlock`，则说明此目录并没有上锁，允许创建或删除
   - 如果为`locked`，则说明此目录上有锁，这时检查调用者`ip:port`是否与`FileInfo`中记录的一致，以便确认此调用者是否有权对此目录进行写操作，如果一致，则允许创建或删除，否则，拒绝创建
   - 如果为`cleaning`，说明此时刚刚将锁释放，正在关闭所需文件，对调用者返回正在清锁的错误码，调用者可以进行重试

# 接口

1. message定义

   `StatusCode {`

	   `…...`

       `kLocked`

       `kUnlock`

       `kCleaning`

      `…..`	

   `}`	

   `message LockDirRequest {`

       `optional int64 sequence_id = 1;`
       
       `optional string dir_path = 2;`
       
   `}`

   `message LockDirResponse {`

   	`optional int64 sequence_id = 1;`

      `optional StatusCode status = 2;`

     `}`

   `message UnlockDirRequest {`

      `optional int64 sequence_id = 1;`

      `optional string dir_path = 2;`

      `optional StatusCode status = 3;` 

    `}`

   `message UnlockDirResponse {`

      `optional int64 sequence_id = 1;`

      `optional StatusCode status = 2;`

     `}`

2.对指定目录加锁:

`void LockDir(::google::protobuf::RpcController* controller,`

   `const LockDirRequest* request,`
   
   `LockDirResponse* response,`
 
   `::google::protobuf::Closure* done);`

`request`中存有需要加锁的路径，`response`中的`status`表明加锁是否成功

3.释放掉指定目录的锁

`void UnlockDir(::google::protobuf::RpcController* controller,`

   `const UnlockDirRequest* request,`
   
   `UnlockDirResponse* response,`
   
   `::google::protobuf::Closure* done);`

`request`中存有需要解锁的路径，`response`中的`status`表明是否解锁成功

# 异常处理

1. 对某个目录加完锁后，`NameServer`宕机或重启

   由于锁状态是持久化在`FileInfo`中，HA方案已天然的将`namespace`同步到多台`NameServer`上，所以无论是宕机重机还是切主，都不影响锁信息的正确性。若重启时发现`FileInfo`中锁的状态为`cleaning`，则在`RebuildBlockMapping`时将此目录下所有正在写的文件记录下来，重新进行清锁流程 

# TODO

1. 此方案中，清锁动作为Tera的`Master`主动发出，未来可以考虑`TabletNode`与`NameServer`维持心跳，心跳超时后主动清锁的设计

2. 此方案中，不涉及递归加锁问题，例如对目录`home/work`进行加锁时，并不会对`/home`目录进行加锁
