# Roadmap

## 基本功能
- [x] 基本文件、目录操作（Create/Delete/Read/Write/Rename)
- [x] 多副本冗余、自动副本恢复
- [x] Nameserver高可用
- [ ] Nameserver拆分出Metaserver
- [ ] 磁盘负载均衡
- [ ] chunkserver间动态负载均衡
- [ ] 文件锁
- [x] 简单多地域副本放置
- [ ] 副本放置策略插件化
- [ ] sdk lease
- [ ] 读文件跳过慢节点

## Posix接口
- [x] mount支持
- [ ] fuse lowlevel实现
- [x] 基本读写操作（不包括随机写）
- [x] 小文件随机写, 支持vim，gcc等应用
- [ ] 大文件随机写

## 应用支持
- [x] Tera
- [ ] Shuttle
- [ ] Galaxy
