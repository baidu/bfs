# Design

## 主备master结构
master之间通过一致性协议保持同步，每个master都存储所有信息。<br />
master的数据是落地的，不一定非得全内存，通过LRU内存cache保证热点访问的快速响应。

## 文件分块
文件是否分块决定了能否支持大文件<br />
文件名信息是否存储在master决定了是否支持小文件<br />
折衷是对大文件才分块，小文件(<100G)不分块；创建文件夹时指定可否list，可list的必须存在目录树中，不可list的可以哈希到chunkserver，让chunkserver去维护meta信息。

好吧，我们先来开发一个单master，不支持分块，不支持小文件的fs吧。
