#include "iostat.h"
#include <iostream>
#include <fstream>
#include <common/logging.h>

namespace baidu {
namespace bfs {

bool GetIOStat(const std::string& path, struct IOStat* iostat) {
    std::ifstream stat(path.c_str());
    if (!stat.is_open())  {
        LOG(WARNING, "open proc stat fail: %s", path.c_str());
        return false;
    }
    std::ostringstream tmp;
    tmp << stat.rdbuf();
    stat.close();
    sscanf(tmp.str().c_str(),
            "%lu %lu %llu %lu %lu %lu %llu %lu %lu %lu %lu",
            &iostat->read_ios, &iostat->read_merges,
            &iostat->read_sectors, &iostat->read_ticks,
            &iostat->write_ios, &iostat->write_merges,
            &iostat->write_sectors, &iostat->write_ticks,
            &iostat->in_flight, &iostat->io_ticks,
            &iostat->time_in_queue);
    return true;
}

} //namespace bfs
} //namespace baidu

