# 设计

## 主备master结构
master之间通过一致性协议保持同步，每个master都存储所有信息。<br />
master的数据是落地的，不一定非得全内存，通过LRU内存cache保证热点访问的快速响应。

## 文件分块
文件是否分块决定了能否支持大文件<br />
文件名信息是否存储在master决定了是否支持小文件<br />
折衷是对大文件才分块，小文件(<100G)不分块；创建文件夹时指定可否list，可list的必须存在目录树中，不可list的可以哈希到chunkserver，让chunkserver去维护meta信息。

## 文件meta信息
文件meta信息存储在master会导致master负载过高（内存，cpu）
我们选择对与不分块的文件， 将文件meta存储在chunkserver，如果用户要list，那仅需要访问master，但如果要获取文件大小等信息，那就必须得访问对应的chunkserver了，master仅存储namespace和每个文件在哪些chunkserver上。

## 写数据流
有两个选择：<br />
1. client写一份，然后chunkserver通过复制链创建多副本，优势是client带宽占用低；<br />
2. client一次写多份，优势是可以多些一份，并在写的过程中抛弃慢节点。
考虑将这个做成个选项，两种都支持。


好吧，我们先来开发一个单master，不支持分块，不支持小文件的fs吧。

# 实现

## Namespace

存储我们选择了leveldb，可以简单的将整个目录结构平展开，并提供高效的文件创建、删除和list操作。<br />
实际目录结构:

	/home/dirx/
	          /filex
	      diry/
	          /filey
	/tmp/
	     filez

过去的存储格式为:

	00/
	01/home
	01/tmp
	02/home/dirx
	02/home/diry
	02/tmp/filez
	03/home/dirx/filex
	03/home/diry/filey

这样做的主要目的是高效支持list操作。

现在的存储格式为:

	1home -> 2
	1tmp -> 3
	2dirx -> 4
	2diry -> 5
	3filez -> 6
	4filex -> 7
	5filey -> 8

其中所有数字都是64位整数，目的是支持高效list操作的同时，支持高效的rename操作。  
rename操作中，文件夹名字改变，但编号不变。

## Open for write  
在namespace中写入， 创建块， 分配datanode。  
如果失败，在nameserver回滚。  
数据发往datanode， 中间网络故障可以重连，但一旦close后不可追加写入。  
nameserver向client发放lease，lease失效后，不可以继续写入。

## sdk端写入流程  
1. Write  
    如果_chains_head为空，则同步调用AddBlock  
    向_write_buf中append数据，如果_write_buf满了，调用StartWrite(_write_buf)  
2. StartWrite  
    将_write_buf放入_write_queue中，然后抛出BackgroundWrite事件，++_back_writing  
3. BackgroundWrite  
    如果_write_queue非空 且 最小的sequence小于滑动窗口上限，异步调用WriteBlock  
4. WriteChunkCallback  
    如果WriteBlock失败，++_back_writing， delay一个DelayWriteChunk  
    如果WriteBlock成功，更新滑动窗口  
    如果_write_queue为空，back_writing为0，则唤醒sync，
    调度一个新的BackgroundWrite  
5. DelayWriteChunk  
    异步调用WriteBlock，--_back_writing

## Open for read

在从namespace中获取datanode信息，向随机datanode读取数据。  
读取不被保护，如果读过程中文件被删除，后续读取失败


