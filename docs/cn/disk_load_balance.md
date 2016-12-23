# Disk Load Balance
## 名词介绍
* DataBlock
	* 负责文件块读写管理
	* 一个文件块对应一个DataBlock对象
* Disk
	* 负责磁盘读写的管理
	* 文件元信息管理
	* 磁盘负载统计
	* 一个物理磁盘对应一个Disk对象
* BlockManager
	* 维护Chunkserver上所有DataBlock的列表
	* 提供DataBlock查找功能

## BlockManager
维护Chunkserver上所有DataBlock的列表。负责DataBlock创建，删除，查找的管理。在新的DataBlock被创建是，BlockManager会收集各磁盘的负载情况，选择负载最低的磁盘负责该Block的读写。

## DataBlock
* 写

DataBlock中维护一个滑动窗口。写操作将`data`和对应的`sequence`提交到滑动窗口，窗口根据`sequence`顺序地将`data`中的数据切分成小数据包，顺序放入任务队列。后台线程将任务队列里的数据包依次写到磁盘上。

* 读

读操作会先从文件中读取数据。如果读请求的offset超过了文件的大小，会试图从任务队列中读取数据，以此保证刚写入尚未落盘的数据也可以被读到。

* 删除

DataBlock维护一个引用计数。删除操作会将`delete`标志置位。读写操作检查到标志置位会退出并释放引用计数。引用计数归零时调用析构函数。

## Disk
Disk管理磁盘上文件的元信息，包括文件大小，权限，版本等。除此之外Disk还负责负载的统计，如文件数量，内存占用量，打开文件数，文件访问频度等。




