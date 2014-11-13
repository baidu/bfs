/***************************************************************************
 * 
 * Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 /**
 * @file mutex_test.cpp
 * @author yanshiguang02@baidu.com
 * @date 2014/04/24 15:34:59
 * @brief 
 *  
 **/

#include "mutex.h"
#include "thread_pool.h"
namespace common {

#define ASSERT_EQ(t1,t2) \
    if (t1 != t2) { \
        printf("case fail %s:%d expect:%d actual:%d\n", __func__, __LINE__, t1, t2); \
    } else { \
        printf("case pass %s:%d\n", __func__, __LINE__); \
    }

class MutexTester {
    int _var;
    int _count;
    Mutex _end_lock;
    CondVar* _end_cond;

    Mutex _run_lock;
    Mutex _add_lock;
    CondVar* _run_cond;
public:
    MutexTester() : _var(0), _count(0), _end_cond(NULL), _run_cond(NULL)  {}
    static void* RunFirst(void* arg) {
        MutexTester* x = (MutexTester*)arg;
        {
            MutexLock lock(&x->_run_lock);
            x->_var += 1;
            x->_run_cond->Wait();
            ASSERT_EQ(x->_var, 3);
        }
        {
            printf("try lock end: %s\n", __func__);
            MutexLock _(&x->_end_lock);
            printf("done lock end: %s\n", __func__);
            x->_end_cond->Signal();
        }
        return NULL;
    }
    static void* RunLast(void* arg) {
        MutexTester* x = (MutexTester*)arg;
        {
            MutexLock lock(&x->_run_lock);
            x->_var += 2;
            x->_run_cond->Signal();
        }
        {
            printf("try lock end: %s\n", __func__);
            MutexLock _(&x->_end_lock);
            printf("done lock end: %s\n", __func__);
            x->_end_cond->Signal();
        }
        return NULL;
    }
    static void* Add(void* arg) {
        MutexTester* x = (MutexTester*)arg;
        for (int i=0;i<100000; i++) {
            MutexLock lock(&x->_add_lock);
            x->_count ++;
        }
        printf("try lock end: %s\n", __func__);
        MutexLock _(&x->_end_lock);
        printf("done lock end: %s\n", __func__);
        x->_end_cond->Signal();
        return NULL;
    }
    void RunAllTest() {
        CondVar run_cond(&_run_lock);
        _run_cond = &run_cond;
        CondVar cond_end(&_end_lock);
        _end_cond = &cond_end;

        pthread_t tid[4];
        tid[0] = StartThread(RunFirst, this);
        tid[1] = StartThread(RunLast, this);

        tid[2] = StartThread(Add, this);
        tid[3] = StartThread(Add, this);
        {
            MutexLock _(&_end_lock);
            for(int i=0;i<4;i++) {
                _end_cond->Wait();
            }
        }

        for(int i=0;i<4;i++) {
            pthread_join(tid[i], NULL);
        }
        ASSERT_EQ(_count, 200000);
    }
};

}   /// namespace common

int main() {
    common::MutexTester tester;
    tester.RunAllTest();
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
