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
    static const struct timespec QUEUE_WAIT;

    const uint32_t threadNum;
    const uint32_t queueSize;
    bool shutDown;
    uint32_t queueStart;
    uint32_t queueEnd;

    pthread_mutex_t queueMutex;
    pthread_cond_t queueAddedCond;
    pthread_cond_t queueRemovedCond;

    Runnable** queue;

    pthread_t* threads;

    void* thread_routine(void* executor);

public:
    Executor(uint32_t threadNum, uint32_t queueSize);
    Executor(const Executor& toCopy) = delete;
    Executor(Executor&& toMove) = delete;
    Executor& operator=(const Executor& toCopy) = delete;
    Executor& operator=(Executor&& toMove) = delete;
    virtual ~Executor();

    void addToQueue(Runnable* toAdd);
    void doShutdown();
    void awaitShutdown();

    friend std::ostream& operator<<(std::ostream& os, const Executor& toPrint);
};

#endif /* EXECUTOR_H_ */
