# Design

## 主备master结构
master之间通过一致性协议保持同步，每个master都存储所有信息。<br />
master的数据是落地的，不一定非得全内存，通过LRU内存cache保证热点访问的快速响应。

## 文件分块
文件是否分块决定了能否支持大文件<br />
文件名信息是否存储在master决定了是否支持小文件<br />
折衷是对大文件才分块，小文件(<100G)不分块；创建文件夹时指定可否list，可list的必须存在目录树中，不可list的可以哈希到chunkserver，让chunkserver去维护meta信息。

## 文件meta信息
文件meta信息存储在master会导致master负载过高（内存，cpu）
我们选择对与不分块的文件， 讲文件meta存储在chunkserver，如果用户要list，那仅需要访问master，但如果要获取文件大小等信息，那就必须得访问对应的chunkserver了，master仅存储namespace和每个文件在哪些chunkserver上。

## 写数据流
有两个选择：<br />
1. client写一份，然后chunkserver通过复制链创建多副本，优势是client带宽占用低；<br />
2. client一次写多份，优势是可以多些一份，并在写的过程中抛弃慢节点。
考虑将这个做成个选项，两种都支持。


好吧，我们先来开发一个单master，不支持分块，不支持小文件的fs吧。

namespace存储我们选择了leveldb，可以简单的将整个目录结构平展开，并提供高效的文件创建、删除和list操作。

实际目录结构

	/home/dir1/
	          /file1
	      dir2/
	          /file2
	/tmp/
	     file2

存储格式为

	001/
	002/home
	002/tmp
	003/home/dir1
	003/home/dir2
	003/tmp/file2
	004/home/dir1/file1
	004/home/dir2/file2

这样做的主要目的是高效支持list操作。
