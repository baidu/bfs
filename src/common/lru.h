#ifndef COMMON_LRU_H_
#define COMMON_LRU_H_

#include <list>
#include <boost/function.hpp>
#include <boost/unordered_map.hpp>

#include "mutex.h"

namespace common {

template<typename T1, typename T2>
class LRU {
public:
    typedef boost::function<void (T1&, T2&)> LRUCallback;
    typedef std::list<std::pair<T1, T2> > List;
    typedef typename List::iterator List_Iter;
    typedef boost::unordered_map<T1, List_Iter> Map;
    typedef typename Map::iterator Map_Iter;
    LRU(int32_t capacity, LRUCallback callback) :
        capacity_(capacity), size_(0), callback_(callback) {
    }
    ~LRU() {
        for (List_Iter list_it = list_.begin();
                list_it != list_.end(); ++list_it) {
            callback_(list_it->first, list_it->second);
        }
    }
    T2* Find(const T1& key) {
        MutexLock lock(&mu_);
        Map_Iter map_it = index_map_.find(key);
        if (map_it == index_map_.end()) {
            return NULL;
        } else {
            List_Iter list_it = map_it->second;
            list_.splice(list_.begin(), list_, list_it);
            return &(list_.begin()->second);
        }
    }
    void Remove(const T1& key) {
        MutexLock lock(&mu_);
        Map_Iter map_it = index_map_.find(key);
        if (map_it != index_map_.end()) {
            List_Iter list_it = map_it->second;
            callback_(list_it->first, list_it->second);
            list_.erase(list_it);
            index_map_.erase(map_it);
            size_--;
        }
    }
    void Insert(const T1& key, const T2& value) {
        MutexLock lock(&mu_);
        Map_Iter map_it = index_map_.find(key);
        if (map_it != index_map_.end()) {
            // already exist
            List_Iter list_it = map_it->second;
            list_.splice(list_.begin(), list_, list_it);
            list_.begin()->second = value;
        } else {
            if (size_ == capacity_) {
                List_Iter list_it = list_.end();
                list_it--;
                callback_(list_it->first, list_it->second);
                list_.splice(list_.begin(), list_, list_it);
                index_map_.erase(list_it->first);
                list_it->first = key;
                list_it->second = value;
                index_map_[key] = list_it;
            } else {
                list_.push_front(std::make_pair(key, value));
                index_map_[key] = list_.begin();
                size_++;
            }
        }
    }
    int32_t Size() const {
        MutexLock lock(&mu_);
        return size_;
    }
private:
    int32_t capacity_;
    int32_t size_;
    List list_;
    Map index_map_;
    LRUCallback callback_;
    Mutex mu_;
};
}
#endif
