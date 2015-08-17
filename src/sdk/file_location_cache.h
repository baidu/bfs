#ifndef BFS_FILE_LOCATION_CACHE_H_
#define BFS_FILE_LOCATION_CACHE_H_

#include <common/cache.h>
#include <proto/nameserver.pb.h>

#include <string>
#include <vector>

namespace bfs {

class FileLocationCache {
public:
    FileLocationCache(int32_t cache_size);
    ~FileLocationCache();
    bool GetFileLocation(const std::string& file_name, std::vector<LocatedBlock>* location);
    void FillCache(const std::string& file_name, const std::vector<LocatedBlock>& location);
private:
    common::Cache::Handle* FindFile(const std::string& file_name);
private:
    common::Cache* _cache;
};

} // namespace bfs

#endif // BFS_FILE_LOCATION_CACHE_H_
