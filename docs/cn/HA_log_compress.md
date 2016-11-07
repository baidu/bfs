# HA 日志压缩
##背景
Nameserver集群间通过一致性协议同步状态机，一个节点宕机后需要通过redo log赶上进度。但是当一台机宕机过久，或者新节点加入时，log过长会导致redo过程太长，影响集群可用性。因此，需要对节点log进行整理，按照一定策略对状态机做快照，并删除掉快照时间时间点之前的log。
##设计
* 日志同步流程（master/slave模式）
	1. master写logdb
	2. master对状态机打快照，并用本条log的id作为快照标识
	3. master写状态机
	4. 将log同步到slave
	5. slave写logdb
	6. slave写状态机
	7. slave返回同步成功，master删除本地对应的状态机快照
	
(i, ii)情况下master宕机，logdb中会有未提交数据。
(iii, iv)情况下master宕机，logdb和状态机中均会有未提交数据。
(v-vii)请情况下master宕机，无脏数据。
简单判断：master宕机并且以从的身份重启时，如果sync_idx != current_idx，则需要清理所有数据拖镜像。

* 拖镜像

镜像由master主动同步给slave。master在试图给从同步某条log时发现slave已经落后太多，就会先将镜像同步给slave，再从镜像时间点开始同步log给slave。

镜像的同步主要依靠两个变量：`snapshot_id` 和 `snapshot_step`。

`snapshot_id`标识镜像的版本，持久化在logdb中。master在向slave同步log的时候如果`snapshot_id`不一致，则需要先将当前镜像同步给slave。每个镜像会分多次同步给slave，每一次由`snapshot_step`标识。`snapshot_step`不持久化，如果在同步过程中slave宕机，则本次同步失败，slave重启后重新同步。镜像同步完成后，slave会将本次镜像的`snapshot_id`持久化在logdb中。


