Garbage Collection
======

nameserver和chunkserver都使用垃圾收集来避免不必要的资源占用。

##nameserver  
##### block副本回收  
如果一个block的副本数超过了用户指定的副本数，nameserver需要发现，并通知chunkserver删除这个block的某个副本。  
这里有多种选择：  
1. 在收到chunkserver的block_report时，检查对应的block，副本数是否超出预期，如果超出，那么通知chunkserver删除。  
2. 在用户减少文件副本数时，发起block副本删除。  
3. 定期扫描所有block，对于超出预期副本数的block，发起副本删除。  

##### 垃圾block的回收  
如果一个文件被删除了，那么它所包含的block都将成为垃圾block，nameserver需要发现，并通知所有包含这个block的chunkserver删除。

##chunkserver  
1. 如果磁盘上的某个block不是合法的block（meta中没有或者与meta不一致），chunkserver应该清理这个block
