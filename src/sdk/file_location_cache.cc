#include "file_location_cache.h"

#include "common/logging.h"

namespace bfs {

struct FileLocationEntiry {
    std::string file_name;
    std::vector<LocatedBlock> block_chains;
};

static void DeleteEntry(const common::Slice& key, void* value) {
    FileLocationEntiry* file = reinterpret_cast<FileLocationEntiry*>(value);
    LOG(INFO, "FileLocation Cache release %s", file->file_name.c_str());
    delete file;
}

FileLocationCache::FileLocationCache(int32_t cache_size) :
    _cache(common::NewLRUCache(cache_size)) {
}

FileLocationCache::~FileLocationCache() {
    delete _cache;
}

common::Cache::Handle* FileLocationCache::FindFile(const std::string& file_name) {
    return _cache->Lookup(file_name);
}

bool FileLocationCache::GetFileLocation(const std::string& file_name,
                                        std::vector<LocatedBlock>* location) {
    bool ret = false;
    common::Cache::Handle* handle = FindFile(file_name);
    if (handle) {
        FileLocationEntiry* file_location =
            reinterpret_cast<FileLocationEntiry*>(_cache->Value(handle));
        for (size_t i = 0; i < file_location->block_chains.size(); i++) {
            location->push_back(file_location->block_chains[i]);
        }
        _cache->Release(handle);
        ret = true;
    }
    return ret;
}

void FileLocationCache::FillCache(const std::string& file_name,
        const std::vector<LocatedBlock>& blocks) {
    common::Slice key(file_name);
    FileLocationEntiry* file = new FileLocationEntiry;
    for (size_t i = 0; i < blocks.size(); i++) {
        file->block_chains.push_back(blocks[i]);
    }
    common::Cache::Handle* handle = _cache->Insert(key, file, 1, &DeleteEntry);
    _cache->Release(handle);
}

}
