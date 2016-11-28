# BFS Master 高可用设计方案
## 背景
在BFS中，nameserver负责管理文件元信息以及所有chunkserver的状态信息，是整个系统中唯一拥有全局信息的模块，同时也成为了系统的单点。nameserver的不可用会直接导致文件系统的瘫痪，于是提高nameserver的可用性至关重要。  
## 整体设计
如下图所示，我们将nameserver扩展为集群，Client只与集群Leader进行交互。每一个nameserver中有一个同步模块（Sync），负责集群间的状态同步，保证元数据的一致性。  
<img src="https://github.com/bluebore/bfs/blob/master/resources/images/ha-1.png" width = "600" height = "200" alt="图片名称" align=center />

* Sync模块  
Sync模块主要有两个功能，选主和日志同步。选主操作就是在nameserver中确定唯一一个Leader，并且在Leader异常后迅速选出新任Leader。日志同步操作实际上就是同步Nameserver中所有需要落地的数据写操作。Sync模块设计为与Nameserver松耦合，仅暴露必须接口，内部可以采用任意一致性协议实现。这样的设计使得我们可以实现多种一致性方案，以满足不同程度的可用性及性能需求。当前我们采用Raft协议实现。  

* Leader  
Nameserver集群中的Leader负责接收和响应Client的请求，并把所做出的决定通过Sync通知集群中其他Nameserver。Leader只在通知成功后才会向Client返回操作成功。  

* 从Nameserver  
Nameserver中的从实例不直接与Client进行交互，也不会给Chunkserver发出任何指令。它只将从Sync模块接收到的命令落地。但是从Nameserver和Leader一样，会接收并处理Chunkserver的心跳和Report消息，维护文件在Chunkserver上分布及Chunkserver状态等信息。这样做的目的是为了在Leader故障后，任何一个从Nameserver可以在最短时间内成为Leader并提供服务。这部分内存信息不与Leader进行一致性检查，原因是这部分信息的来源是Chunkserver，只有Chunkserver才是真理的唯一掌握者，与Leader的内存状态保持一致并没有意义。

##主要操作流程方案一
* 写  
在BFS中，有三种操作会产生元数据写操作：Create，Delete和Rename。操作流程如下图所示，  
<img src="https://github.com/bluebore/bfs/blob/master/resources/images/ha-2.png" width = "400" height = "400" alt="图片名称" align=center />


Client向Leader Nameserver发起请求，Leader收到请求后，检查操作合法性。如果操作是合法的，Leader将需要落地的数据通过Sync扩散给从Nameserver。Sync模块返回扩散成功后，Leader向Client返回操作成功。  
检查合法性可以并发执行，串行向Sync提交结果，Sync模块严格按照这个顺序扩散，扩散成功后Leader才可以将结果写进数据库。但是这样Leader的性能瓶颈就在于Sync的扩散，当Nameserver集群跨机房跨地域部署时，这样的延时是不能接受的。于是我们对这里进行了如下图所示的优化。  
<img src="https://github.com/bluebore/bfs/blob/master/resources/images/ha-3.png" width = "200" height = "400" alt="图片名称" align=center />

Leader在收到操作后先对数据库打一个快照，检查完合法性后，在提交给Sync之前就将数据写入本地数据库。之后再将操作提交给Sync，Sync返回扩散成功后，Leader将快照删除并给向Client返回成功。在Sync返回成功前，所有的读请求都会读快照之前的数据，也就是说Client不会读到还未扩散成功的数据。  

扩散的失败，例如RPC超时、写失败等需要重试处理，如果超过一定重试次数，则均视为Nameserver集群不可用，需人工介入处理。各一致性协议对于扩散失败的定义不同，例如Raft中，大于半数成员收到消息便认为扩散成功；主从模式中主或从任意一方写失败均认为是扩散失败。  

* 读  
因为数据扩散存在延时，只有Leader掌握最新的数据，所以只有Leader可以响应Client的读请求。Client向Nameserver集群中任一实例发起读请求，如果正好是Leader接收到了请求，则直接返回给Client结果；否则，从Nameserver返回读失败并告知当前Leader地址，Client向新Leader请求。  

* 重启  
因为我们采取了先写入数据库后扩散的方式，所以Leader宕机重启后，数据库中可能存在脏数据。我们不记录Nameserver宕机前的身份，对Leader和从Nameserver宕机后的重启做同样的处理：从Leader拷贝数据库镜像，然后redo镜像后的操作log。

## 主要操作流程方案二
Client向Leader Nameserver发起请求，Leader收到请求后，对所需操作的路径加锁（加锁逻辑见另一文档），检查操作合法性。如果操作是合法的，Leader将需要落地的数据通过Sync扩散给从Nameserver。Sync模块返回扩散成功后，Leader向Client返回操作成功。Follower收到提交操作的指令后，无需检查操作合法性，直接根据指令执行操作更改状态机和内存结构。

对于某一操作，具体流程如下：

1. Leader		收到请求，检查请求合法性

2. Leader		将操作指令序列化并交由Sync同步

3. Follower		收到指令后持久化指令

4. Both			确定操作同步成功后将操作应用到状态机，并更改内存状态

方案一为同步结果，方案二为同步操作。同步结果的问题在于当操作的结果很大（例如，`rmr /`），可能超出内存大小范围，从而很难保证操作原子性。同步操作基于一个假设：Leader和Followers将同一个指令应用到状态机及更改内存状态所产生的结果严格一致。


##一期高可靠方案 - 主从模式
Nameserver的HA方案数据流如上描述，其中Sync模块是保证数据可靠和高可用的核心。我们把这个模块设计成可插拔的插件模式，可以选用任意一种一致性协议实现以满足不同可靠、可用和性能要求。一期方案是实现一个主从模式的Nameserver，首先保证数据的可靠性，在一定程度上提高可用性。  

* 写  
写流程如下图所示，和上面描述的一致。主从模式的Sync实现为简单的RPC调用，即主Nameserver将需要扩散的操作通过RPC发送给从Nameserver；从Nameserver收到RPC后不做任何合法性检查，直接更新内存状态及本地持久化Namespace（leveldb）；操作成功后调用RPC回调通知主Nameserver；主Nameserver收到成功的回调后向用户返回操作成功。  
<img src="https://github.com/bluebore/bfs/blob/master/resources/images/ha-4.png" width = "300" height = "550" alt="图片名称" align=center />

* 读  
读流程和上面描述的一致。

* 主从  
这个方案中，主从任何一台机器宕机的情况下，对Nameserver的写操作都会失败，这时需要人工介入处理。没有加入检测宕机并自动切换主从的逻辑，原因是在没有一致性选主协议的情况下，几乎很难保证主从探活策略的正确性，很容易出现无主或者双主的情况。这种设计保证了数据的可靠性，但是在一定程度上牺牲了可用性。为了减小不可用时间，我们加入了一种单写模式，即对Nameserver的写操作只需在主Nameserver上更新成功。当处理完宕机，集群恢复主从都正常运行的情况下后，需切换回双写模式，以保证数据可靠性。主从切换操作流程如下： 
<img src="https://github.com/bluebore/bfs/blob/master/resources/images/ha-5.png" width = "300" height = "300" alt="图片名称" align=center />


