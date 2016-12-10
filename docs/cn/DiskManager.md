# DiskManager
## 名词介绍
* DataBlock
	负责文件读写，一个文件对应一个DataBlock对象。
* Disk
	负责磁盘上文件(DataBlock对象)的管理，文件元信息管理，磁盘负载统计。一个物理磁盘对应一个Disk对象。
* DiskManager
	负责磁盘(Disk对象)管理，磁盘间负载均衡及调度。一个Chunkserver有一个DiskManager对象。

## DataBlock
* 写
DataBlock中维护一个滑动窗口。写操作将`data`和对应的`sequence`提交掉滑动窗口，窗口根据`sequence`顺序地将`data`中的数据切分成小数据包，顺序放入任务队列。后台线程将任务队列里的数据包依次写到磁盘上。
* 读
读操作会先从文件中读取数据。如果读请求的offset超过了文件的大小，会试图从任务队列中读取数据，以此保证刚写入尚未落盘的数据也可以被读到。
* 删除
DataBlock维护一个引用计数。删除操作会将`delete`标志置位。读写操作检查到标志置位会退出并释放引用计数。引用计数归零时调用析构函数。

## Disk
Disk管理磁盘上文件的元信息，包括文件大小，权限，版本等。除此之外Disk还负责负载的统计，如文件数量，所有DataBlock任务队列大小(内存占用量)，打开文件数，文件访问频度等。

## DiskManager
当有新的文件创建时，DiskManager会汇总所有Disk的负载信息，挑选出负载最轻的Disk。当某一块磁盘下线时，DiskManager会将该磁盘对应的内存信息删除并析构对应的Disk结构。除此之外，DiskManager还负责一些策略调度，例如当某一文件访问频度很高的时候，DiskManager会将该文件缓存在SSD中。

