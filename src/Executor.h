/*
 * Executor.h
 *
 *  Created on: 4 Ιαν 2019
 *      Author: pibre
 */

#ifndef EXECUTOR_H_
#define EXECUTOR_H_
class Executor;

#include <ostream>

#include <pthread.h>

#include "Runnable.h"

class Executor {
protected:
    const std::string name;
    const timespec queueWait;
    const uint32_t threadNum;
    const uint32_t queueSize;
    bool shutDown;
    uint32_t queueStart;
    uint32_t queueEnd;
    uint32_t used;

    pthread_mutex_t queueMutex;
    pthread_condattr_t attr;
    pthread_cond_t queueAddedCond;
    pthread_cond_t queueRemovedCond;

    Runnable** queue;

    pthread_t* threads;

    void* thread_routine(void* ignored);

public:
    Executor(uint32_t threadNum, uint32_t queueSize, const std::string name);
    Executor(const Executor& toCopy) = delete;
    Executor(Executor&& toMove) = delete;
    Executor& operator=(const Executor& toCopy) = delete;
    Executor& operator=(Executor&& toMove) = delete;
    virtual ~Executor();

    void addToQueue(Runnable* toAdd);
    void doShutdown();
    void awaitShutdown();
    std::string getName() const;

    friend std::ostream& operator<<(std::ostream& os, const Executor& toPrint);
};

#endif /* EXECUTOR_H_ */
