# Namespace For FUSE Lowlevel
## 需求
根据fuse lowlevel的需求，需要两种获得文件的fileinfo的方式：

    *  iget：entry_id->fileinfo（目前版本的BFS未实现）
    *  lookup：parent_id+name->fileinfo（目前版本的BFS在namespace中已实现）

## 方案
根据以上需求，有以下两种方案:

`方案一：再添加一个db来存entry_id->fileinfo的信息。优点：实现简单，对现有的namespace结构改动较小；缺点：有两个db的一致性的问题，存储的信息有冗余，空间利用率低。`

`方案二：将现有的用来存parent_id+name->fileinfo的db_用来存entry_id->fileinfo。然后再用iget来实现lookup。优点：没有两个db的一致性问题；缺点：对现有namespace的结构改动较大，且这种方式的lookup比目前版本的lookup要慢。`

## 改动
为了快速实现fuse lowlevel的接口，所以选择对现有namespace改动较小的方案一，对目前的namespace暂时改动如下：

1.增加一个表`db_i`来维护`entry_id->fileinfo`，`db_i`中的`fileinfo`只用`name`字段，且该`name`字段存的是文件的`path`。

2.添加新的接口`bool NameSpace::IGet(int64_t entry_id, FileInfo * info)`。

3.修改其他接口的实现，如`DeleteFileInfo`，`BuildPath`，`CreateFile`等。

## 举例
如fuse lowlevel的`getattr`操作流程：由`entry_id`在db_i里获得`path`，获得文件的`path`以后再调用namespace现有的接口获得文件的`fileinfo`（entry_id->path->fileinfo）。

## 说明
该namespace临时方案对fuse lowlevel只支持单NS，无法支持NS之间的接替。因为对db_i操作的`logentry`没有写到log里，也没有针对`db_i`的情况对`NameSpace::TailLog`函数进行相应的修改。


