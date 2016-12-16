
# 文件软链接－设计
## 整体设计
1. 在 FileInfo 中添加 sym_link，标识 软链接路径
      原字段 type，0表示普通文件，1表示目录，增加 2表示软链接
2. 在创建软链接时，首先检查 old_path是否存在
      >* 如果存在，则对new_path进行检查并对父目录不存在时进行创建，成功后ldb中增加kv；失败返回；
      >* 如果不存在，则返回；

3. 读文件时，如果是“软链接”，则把路径重定向到“原路径”下，再进行后续步骤；
4. 删除文件时，如果是“软链接”，则删除ldb中该记录；不对其 指向的路径做任何操作；
5. 写文件时，如果是“软链接”路径下，上传文件 时，均指向“原路径”，再进行后续步骤；
6. 在ns启动过程中，对副本检查和恢复时，当文件类型是“软链接”时，跳过检查；

## 接口：
### message定义：
```
message FileInfo {
    optional int64 entry_id = 1;
    optional int64 version = 2;
    optional int32 type = 3 [default = 0755];
    repeated int64 blocks = 4;
    optional uint32 ctime = 5;
    optional string name = 6;
    optional int64 size = 7;
    optional int32 replicas = 8;
    optional int64 parent_entry_id = 9;
    optional int32 owner = 10;
    repeated string cs_addrs = 11;
    optional string sym_link = 12; //新增字段
}
```
```
message SymlinkRequest {
    optional int64 sequence_id = 1;
    optional string old_path = 2;
    optional string new_path = 3;
    optional int32 mode = 4;
    optional string user = 5;
}
```
```
message SymlinkResponse {
    optional int64 sequence_id = 1;
    optional StatusCode status = 2;
}
```
### 创建软链接，需要增加接口
```
@bfs_client
int BfsCreateSymlink(baidu::bfs:FS*fs, int argc, char* argv[]);
```
```
@bfs.h
virtual int32_t Symlink(const char* src, const char* dst) = 0;
```
```
@fs_impl
int32_t Symlink(
    const char* src,
    const char* dst);
```
```
@nameserver_impl
void Symlink(
    ::google::protobuf::RpcController* controller,
    const SymlinkRequest* request,
    SymlinkResponse* response,
    ::google::protobuf::Closure* done);
```
```
@namespace
Symlink(
    const std:string& src,
    const std::string& dst,
    int mode,
    NameServerLog *log);
```












