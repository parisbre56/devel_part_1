/*
 * Executor.cpp
 *
 *  Created on: 4 Ιαν 2019
 *      Author: pibre
 */

#include "Executor.h"

using namespace std;

const struct timespec Executor::QUEUE_WAIT = { 1, 0 };  // Definition

Executor::Executor(uint32_t threadNum, uint32_t queueSize) :
        threadNum(threadNum),
        queueSize(queueSize),
        shutDown(false),
        queueStart(0),
        queueEnd(0),
        queueMutex(PTHREAD_MUTEX_INITIALIZER),
        queueAddedCond(PTHREAD_COND_INITIALIZER),
        queueRemovedCond(PTHREAD_COND_INITIALIZER),
        queue(new Runnable*[queueSize]),
        threads(new pthread_t[threadNum]) {
    for (uint32_t i = 0; i < threadNum; ++i) {
        pthread_create(&(threads[i]),
                       nullptr,
                       (void *(*)(void*))&Executor::thread_routine, this);
    }
}

Executor::~Executor() {
    doShutdown();
    awaitShutdown();
    delete[] queue;
    delete[] threads;
}

void* Executor::thread_routine(void* executor) {
    while (true) {
        pthread_mutex_lock (&queueMutex);
        if (shutDown) {
            pthread_mutex_unlock(&queueMutex);
            break;
        }
        if (queueStart == queueEnd) {
            pthread_cond_timedwait(&queueAddedCond, &queueMutex, &QUEUE_WAIT);
            //If we have shut down while waiting, stop
            if (shutDown) {
                pthread_mutex_unlock(&queueMutex);
                break;
            }
            //If we still have nothing to read, loop so we can wait again
            if (queueStart == queueEnd) {
                pthread_mutex_unlock(&queueMutex);
                continue;
            }
        }
        //Else, we have something to read
        Runnable* toRun = queue[queueStart++];
        //Loop pointer around if we reached the end
        if (queueStart == queueSize) {
            queueStart = 0;
        }
        //Unlock and signal so that others can also process
        pthread_mutex_unlock(&queueMutex);
        pthread_cond_signal(&queueRemovedCond);
        //Run the runnable
        toRun->run();
    }
    return nullptr;
}

void Executor::addToQueue(Runnable* toAdd) {
    pthread_mutex_lock(&queueMutex);
    if (shutDown) {
        pthread_mutex_unlock(&queueMutex);
        throw runtime_error("Executor shutdown, can't add runnable");
    }
    //TODO handle edge case of queue full
}

