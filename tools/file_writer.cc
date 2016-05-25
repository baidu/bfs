#include <string>
#include <boost/bind.hpp>

#include <common/thread_pool.h>

#include "sdk/bfs.h"

char* nameserver_address = NULL;
baidu::bfs::FS* fs = NULL;
baidu::common::ThreadPool* thread_pool = new baidu::common::ThreadPool(20);

std::string generatoe_name(int32_t num) {
    char name[20];
    snprintf(name, 20, "%04d", num);
    return std::string(name);
}

void write_file(int32_t start_dir_name, int32_t start_file_name, int64_t file_size) {
    std::string dir_name = generatoe_name(start_dir_name);
    std::string file_name = generatoe_name(start_file_name);
    std::string path = std::string("/home/bfs/dir") + dir_name + std::string("/") + file_name;
    baidu::bfs::File* file = NULL;
    if (!fs->OpenFile(path.c_str(), O_WRONLY, 644, 0, &file)) {
        fprintf(stderr, "Open file %s error\n", path.c_str());
        return;
    }
    std::string context = dir_name + file_name;
    int64_t len = 0;
    while (len < file_size) {
        if ((size_t)(file_size - len) < context.size()) {
            file->Write(std::string(file_size - len, 0).data(), file_size - len);
            break;
        }
        int write_len = file->Write(context.data(), context.size());
        if ((size_t)write_len != context.size()) {
            fprintf(stderr, "Write file %s error\n", path.c_str());
            file->Close();
            delete file;
            return;
        }
        len += write_len;
    }
    file->Close();
    delete file;
}

int main(int argc, char* argv[])
{
    if (argc != 5) {
        printf("Usage: <nameserver address> <dir num> <file num for each dir> <file size>\n");
        return -1;
    }
    nameserver_address = argv[1];
    int dir_num = atoi(argv[2]);
    int file_num_for_each_dir = atoi(argv[3]);
    int64_t file_size = atoi(argv[4]);
    if (!baidu::bfs::FS::OpenFileSystem(nameserver_address, &fs)) {
        fprintf(stderr, "Open file system %s fail\n", nameserver_address);
        return -1;
    }
    for (int i = 0; i < dir_num; i++) {
        for (int j = 0; j < file_num_for_each_dir; j++) {
            thread_pool->AddTask(boost::bind(&write_file, i, j, file_size));
        }
    }
    thread_pool->Stop(true);
    delete thread_pool;
}
