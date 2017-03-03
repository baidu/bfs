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
写流程和上面描述的一致。主从模式的Sync实现为简单的RPC调用，即主Nameserver将需要扩散的操作通过RPC发送给从Nameserver；从Nameserver收到RPC后不做任何合法性检查，直接更新内存状态及本地持久化Namespace（leveldb）；操作成功后调用RPC回调通知主Nameserver；主Nameserver收到成功的回调后向用户返回操作成功。  

* 读  
读流程和上面描述的一致。

* 主从模式及切换
主从方案有两种模式：主从模式和单主模式。严格的主从模式，需要日志成功同步给从后再向用户返回成功。这种情况一旦从宕机，会导致集群不可用。所以引入了单主模式，在从故障的情况下，依然可以提供服务。单主模式下，主将日志持久化到本地log，之后直接向用户返回结果。

当主宕机时，需要将从切换为主。这是向从发送切换命令，从将自己的`term`加一，然后切换为主对外提供服务。`term`的作用主要为了标识曾经发生过主从切换，因为当前实现中，主的状态机中可能存在脏数据，之前的主作为从重启，而又没有清理自己的状态机，脏数据将不会被发现。在`term`机制下，之前的主以从的身份重启后，会发现有比自己`term`更高的主存在，会将自己的状态机和本地日志清理干净，然后等待主发送镜像。

* 镜像
主会定期清理本地的log，如果从宕机过久，或者新机器作为从连入集群，则需要通过发送镜像的方式将状态机同步给从。其中`snapshot_seq`为一个内存变量，主要用来防止中途从宕机，导致数据错乱。
	1. 主在namespace上打快照
	2. 主扫描namespace，根据配置大小打包，发送给从。
	3. 从收到快照包后检查`snapshot_seq`，如果`snapshot_seq`为零，说明是一个新的镜像，从将本地namespace和log清理干净。如果`snapshot_seq`和从记录的`snapshot_seq`不一致，则返回失败。
	4. 从将包内的内容一条一条写入namespace。之后将`snapshot_seq`加一，返回成功。
	5. 全部镜像发送成功后主将namespace上的快照释放。
	6. 主从开始同步镜像的时间点，将之后的log同步给从，直到从追上主。



