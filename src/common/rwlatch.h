//
// Created by Anti on 2023/7/17.
//

#ifndef RMDB_RWLATCH_H
#define RMDB_RWLATCH_H
#pragma once

#include <iostream>
#include <thread>
#include <condition_variable>
#include <mutex>
#include <string>
#include <vector>

/**
 * @description 读写锁
 *
 * @use 通过read_lock, read_unlock,  write_lock, write_unlock 对page进行读写锁操作
 * @see rwlatch_test中的测试用例
 *
 * @reference https://stackoverflow.com/questions/12033188/how-would-you-implement-your-own-reader-writer-lock-in-c11
 * @reference https://github.com/kw90/ReadWriteLock
 * @reference https://www.educative.io/blog/modern-multithreading-and-concurrency-in-cpp
 * @reference https://github.com/Kevin-ziyue/RWLatch/blob/master/rwlatch.hpp
 */
class RWLatch {

public:
    void read_lock() {
        std::unique_lock<std::mutex> unq_mtx(mtx);
        while (write_locked){
            can_read_or_write.wait(unq_mtx);  // wait under notification(notify-all)
            // Atomically unlocks lock, blocks the current executing thread. A notify will allow it to lock the lock, and unblocks the current thread.
        }
        reader_cnt++;
        can_unlock.notify_all();
    }

    void read_unlock(){
        std::unique_lock<std::mutex> unq_mtx(mtx);
        while(reader_cnt < 1){ // check(AntiO2) 此处应该时reader_cnt >= 1 ?
            can_unlock.wait(unq_mtx);
        }
        reader_cnt --;
        if(reader_cnt == 0){
            can_read_or_write.notify_all();
        }
    }

    void write_lock(){
        std::unique_lock<std::mutex> unq_mtx(mtx);
        while(reader_cnt || write_locked){
            can_read_or_write.wait(unq_mtx);
        }
        write_locked = true;
        can_unlock.notify_all();
    }

    void write_unlock(){
        std::unique_lock<std::mutex> unq_mtx(mtx);
        while(!write_locked){
            can_unlock.wait(unq_mtx);
        }
        write_locked = false;
        can_read_or_write.notify_all();  // notify all waits
    }

    [[nodiscard]] size_t getReaderCnt() const {
        return reader_cnt;
    }

private:
    std::mutex mtx;         // mutex for critical section
    bool write_locked = false;
    size_t reader_cnt = 0;
    std::condition_variable can_read_or_write;
    std::condition_variable can_unlock;
};


#endif //RMDB_RWLATCH_H