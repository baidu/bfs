# 文件分块
## 流程
* 写
	1. Chunkserver端判断文件块已经到大小限制。将当前request写入，并close当前DataBlock，向SDK返回相应状态码。
	2. SDK若收到文件块大小到限制的状态码，则向Nameserver发起AddBlock操作。
	3. bg_error时发起AddBlock写新文件块。
* 读
	1. OpenFile时获取所有文件块的位置和大小信息。


## 问题
1. 为处理重试问题，当前AddBlock会把之前add过的内容清除。在文件分块时需区分这两种情况。
2. Nameserver收到AddBlock请求时，需判断是否来源于同一SDK，以防多SDK写同一文件的情况发生。
3. 如果SDK在写失败(Chunkserver pending过多或宕机情况)时发起AddBlock写新文件块，在批量Chunkserver有问题的情况下可能会有很多小文件块，会不会造成问题。




