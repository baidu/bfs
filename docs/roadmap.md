# Roadmap

## Basic functions
- [x] Basic files, directory operations(Create/Delete/Read/Write/Rename)
- [x] automatic recovery
- [x] Nameserver HA
- [ ] Split the Metaserver from the Nameserver
- [ ] disk loadbalance
- [ ] Dynamic load balancing of chunkserver
- [ ] File Lock & Directory Lock
- [x] Simple multi-geographical replica placement
- [ ] sdk lease
- [ ] Skip slow nodes while reading a file

## Posix interface
- [x] mount support
- [ ] fuse lowlevel
- [x] Basic read and write operations（not include random writes）
- [x] Small file random write, support vim, gcc and other applications
- [ ] Large file random write

## Application support
- [x] Tera
- [ ] Shuttle
- [ ] Galaxy
