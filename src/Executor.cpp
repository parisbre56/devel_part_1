/*
 * Executor.cpp
 *
 *  Created on: 4 Ιαν 2019
 *      Author: pibre
 */

#include "Executor.h"

#include "ConsoleOutput.h"

using namespace std;

const struct timespec Executor::QUEUE_WAIT = { 1, 0 };  // Definition

Executor::Executor(uint32_t threadNum, uint32_t queueSize) :
        threadNum(threadNum),
        queueSize(queueSize),
        shutDown(false),
        queueStart(0),
        queueEnd(0),
        used(0),
        threadIdGenerator(0),
        queueMutex(PTHREAD_MUTEX_INITIALIZER),
        queueAddedCond(PTHREAD_COND_INITIALIZER),
        queueRemovedCond(PTHREAD_COND_INITIALIZER),
        queue(new Runnable*[queueSize]),
        threads(new pthread_t[threadNum]) {
    for (uint32_t i = 0; i < threadNum; ++i) {
        pthread_create(&(threads[i]),
                       nullptr,
                       reinterpret_cast<void *(*)(void*)>(&Executor::thread_routine), this);
    }
}

Executor::~Executor() {
    doShutdown();
    awaitShutdown();
    pthread_cond_destroy(&queueRemovedCond);
    pthread_cond_destroy(&queueAddedCond);
    pthread_mutex_destroy(&queueMutex);
    delete[] queue;
    delete[] threads;
}

void* Executor::thread_routine(void* ignored) {
    //TODO more deadlock safety? use robust mutex?
    uint32_t runnableCount = 0;
    pthread_mutex_lock(&queueMutex);
    const uint32_t threadId = threadIdGenerator++;
    ConsoleOutput consoleOutput("Executor::thread_routine_"
                                + to_string(threadId));
    pthread_mutex_unlock(&queueMutex);
    CO_IFDEBUG(consoleOutput, "Started executor");

    while (true) {
        pthread_mutex_lock(&queueMutex);
        if (shutDown) {
            CO_IFDEBUG(consoleOutput, "Shutdown, exiting...");

            pthread_mutex_unlock(&queueMutex);
            break;
        }
        //If queue is empty, wait for data
        if (used == 0) {
            pthread_cond_timedwait(&queueAddedCond, &queueMutex, &QUEUE_WAIT);
            //If we have shut down while waiting, stop
            if (shutDown) {
                CO_IFDEBUG(consoleOutput, "Shutdown after waiting, exiting...");
                pthread_mutex_unlock(&queueMutex);
                break;
            }
            //If we still have nothing to read, loop so we can wait again
            if (used == 0) {
                pthread_mutex_unlock(&queueMutex);
                continue;
            }
        }
        //Else, we have something to read
        Runnable* toRun = queue[queueEnd++];
        used--;
        //Loop pointer around if we reached the end
        if (queueEnd == queueSize) {
            queueStart = 0;
        }
        //Unlock and signal so that others can also process
        pthread_mutex_unlock(&queueMutex);
        pthread_cond_signal(&queueRemovedCond);
        //Run the runnable
        CO_IFDEBUG(consoleOutput,
                   "Executing runnable [runnableCount="<<runnableCount<<", toRun="<<*toRun<<"]");
        runnableCount++;
        try {
            toRun->run();
        }
        catch (const exception& ex) {
            consoleOutput.errorOutput() << "Error occurred: "
                                        << ex.what()
                                        << endl;
            doShutdown();
            break;
        }
        catch (...) {
            consoleOutput.errorOutput() << "Unknown failure occurred" << endl;
            doShutdown();
            break;
        }
    }
    pthread_exit(nullptr);
    return nullptr;
}

void Executor::addToQueue(Runnable* toAdd) {
    if (toAdd == nullptr) {
        throw runtime_error("Can't add null runnable to queue");
    }
    while (true) {
        pthread_mutex_lock(&queueMutex);
        if (shutDown) {
            pthread_mutex_unlock(&queueMutex);
            throw runtime_error("Executor shutdown, can't add runnable");
        }
        //If queue is full, wait for someone to remove things
        if (used == queueSize) {
            pthread_cond_timedwait(&queueRemovedCond, &queueMutex, &QUEUE_WAIT);
            //If we have shut down while we were waiting
            if (shutDown) {
                pthread_mutex_unlock(&queueMutex);
                throw runtime_error("Executor shutdown, can't add runnable");
            }
            //If the queue is still full, loop so we can wait again
            if (used == queueSize) {
                pthread_mutex_unlock(&queueMutex);
                continue;
            }
        }
        break;
    }
//Else, there's room in the queue
    queue[queueStart++] = toAdd;
    used++;
//Loop pointer around if we have reached the end
    if (queueStart == queueSize) {
        queueStart = 0;
    }
//Unlock and signal so that others can also process
    pthread_mutex_unlock(&queueMutex);
    pthread_cond_signal(&queueAddedCond);
}

void Executor::doShutdown() {
    pthread_mutex_lock(&queueMutex);
    shutDown = true;
    pthread_mutex_unlock(&queueMutex);
    pthread_cond_broadcast(&queueAddedCond);
}

void Executor::awaitShutdown() {
    for (uint32_t i = 0; i < threadNum; ++i) {
        pthread_join(threads[i], nullptr);
    }
}

ostream& operator<<(ostream& os, const Executor& toPrint) {
    os << "[Executor threadNum="
       << toPrint.threadNum
       << ", queueSize="
       << toPrint.queueSize
       << ", shutDown="
       << toPrint.shutDown
       << ", queueStart="
       << toPrint.queueStart
       << ", queueEnd="
       << toPrint.queueEnd
       << ", used="
       << toPrint.used
       << ", threadIdGenerator="
       << toPrint.threadIdGenerator
       << "]";
    return os;
}
