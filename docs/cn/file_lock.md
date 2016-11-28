

# 文件锁支持方案

## 背景

为保证对同一文件元数据操作的正确性，需要将对元数据的操作串行化，所以需要文件锁的支持

## 方案

在`NameServer`中，设置一个`FileLockManager`，用来负责保证单个文件元数据操作的原子性，`FiloLockManager`为每个正在进行元信息修改的文件维护一个内存结构`FileLockInfo`，该结构中包含一个等待队列`waiters_`

当对文件的操作请求到来时，首先调用`FileLockManager`的`GetFileLock`接口，尝试对该文件进行加锁

1. 如果在`FileInfoManager`中并没有找到该文件对应的`FileLockInfo`，则说明并没有其它线程对该文件进行操作，此时构造一个新的`FileLockInfo`，并将其记录到`FileLockManager`中维护的`FileName-->FileLockInfo`的哈希表中，继续进行后续其它操作
2. 如果在`FileInfoManager`中找到该文件对应的`FileLockInfo`，则说明此文件正在被其它线程操作，则直接将当前请求的`request`、`response`插入到`waiters_`队列中，然后返回加锁失败

当对文件的操作请求执行完毕时，调用`FileLockManager`的`ReleaseFileLock`接口，对该文件进行解锁

1. 检查`waiters_`队列是否为空，如果为空，则说明并没有对该文件的其它操作等待执行，调用`FileLockInfo`的析构函数将其析构
2. 如果`waiters_`队列不为空，则说明有对该文件的其它操作等待执行，则将队列头中的`request`、`response`取出，进行rpc调用的处理

## 可能需要的优化

1. 考虑到每个文件的锁相对比较独立，可以对`FileLockManager`中的映射信息进行分桶
2. 绝大部分情况下，并不会出现同时操作同一个文件的元数据的情况，因此次加锁、释放锁的时候，会造成`FileLockInfo`结构的构造和析构，或者可以辅以`cache`，将需要释放的`FileLockInfo`先缓存起来，让缓存去负责最终释放，当需要新申请`FileLockInfo`时，直接从缓存中召回一个即可，类似内核中`slab`的做法

## 特殊情况处理

1. 对于`Rename`操作的处理：由于大多数操作只需要拿到单个文件的锁，所以可以对不同文件进行分桶。然而`rename`操作需要对两个文件进行处理，当其中一个文件加锁失败时，`rename`操作不应该可以继续执行。可以考虑在加锁时增加一个标记位，来标记如果此次加锁不成功，是不是立即放弃。在`rename`加锁时，对两个文件中任何一个加锁失败，即返回操作失败
2. 对于删除目录和创建/删除文件的并发处理


## 存在的问题

如果支持排队等待，则需要将rpc入口时的`MethodDescriptor`向下传到实际的rpc处理函数中去