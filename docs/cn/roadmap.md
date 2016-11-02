# Roadmap

## 基本功能
- [x] 基本的文件、目录操作(Create/Delete/Read/Write/Rename)
- [x] 自动副本恢复
- [x] Nameserver高可用
- [ ] 拆分MetaServer
- [ ] 磁盘间负载均衡
- [ ] chunkserver间动态负载均衡
- [ ] 文件锁和目录锁
- [x] 简单的跨地域副本放置
- [ ] SDK lease
- [ ] 读时慢节点规避
- [ ] 扇出写

## Posix接口
- [x] Mount支持
- [ ] Fuse lowlevel接口实现
- [x] 基本读写操作（不包括随机写）
- [x] 小文件（<256MB）随机写（支持vim, gcc等应用）
- [ ] 大文件随机写

## 应用支持
- [x] Tera
- [x] Shuttle
- [ ] Galaxy 
