/*
 * Executor.cpp
 *
 *  Created on: 4 Ιαν 2019
 *      Author: pibre
 */

#include "Executor.h"

#include "ConsoleOutput.h"

#include <cerrno>
#include <cstring>

using namespace std;

Executor::Executor(uint32_t threadNum, uint32_t queueSize) :
        queueWait( { 10, 0 }),
        threadNum(threadNum),
        queueSize(queueSize),
        shutDown(false),
        queueStart(0),
        queueEnd(0),
        used(0),
        queueMutex(PTHREAD_MUTEX_INITIALIZER),
        queue(new Runnable*[queueSize]),
        threads(new pthread_t[threadNum]) {
    int error = pthread_condattr_init(&attr);
    if (error != 0) {
        throw runtime_error("pthread_condattr_init failed [error="
                            + to_string(error)
                            + "]");
    }
    error = pthread_condattr_setclock(&attr, CLOCK_MONOTONIC); //Unfortunately pthread doesn't like CLOCK_MONOTONIC_COARSE
    if (error != 0) {
        throw runtime_error("pthread_condattr_setclock failed [error="
                            + to_string(error)
                            + "]");
    }
    error = pthread_cond_init(&queueAddedCond, &attr);
    if (error != 0) {
        throw runtime_error("pthread_cond_init added failed [error="
                            + to_string(error)
                            + "]");
    }
    error = pthread_cond_init(&queueRemovedCond, &attr);
    if (error != 0) {
        throw runtime_error("pthread_cond_init removed failed [error="
                            + to_string(error)
                            + "]");
    }
    for (uint32_t i = 0; i < threadNum; ++i) {
        error = pthread_create(&(threads[i]),
                               nullptr,
                               reinterpret_cast<void *(*)(void*)>(&Executor::thread_routine), this);
        if(error != 0) {
            shutDown = true;
            throw runtime_error("pthread_create failed [i="
            +to_string(i)
            +", error="
            + to_string(error)
            + "]");
        }
    }
}

Executor::~Executor() {
    doShutdown();
    awaitShutdown();
    int error = pthread_cond_destroy(&queueRemovedCond);
    if (error != 0) {
        throw runtime_error("pthread_cond_destroy removed failed [error="
                            + to_string(error)
                            + "]");
    }
    error = pthread_cond_destroy(&queueAddedCond);
    if (error != 0) {
        throw runtime_error("pthread_cond_destroy added failed [error="
                            + to_string(error)
                            + "]");
    }
    error = pthread_condattr_destroy(&attr);
    if (error != 0) {
        throw runtime_error("pthread_condattr_destroy failed [error="
                            + to_string(error)
                            + "]");
    }
    error = pthread_mutex_destroy(&queueMutex);
    if (error != 0) {
        throw runtime_error("pthread_mutex_destroy failed [error="
                            + to_string(error)
                            + "]");
    }
    delete[] queue;
    delete[] threads;
}

void* Executor::thread_routine(void* ignored) {
    //TODO more deadlock safety? use robust mutex?
    ConsoleOutput consoleOutput("Executor::thread_routine");
    try {
        CO_IFDEBUG(consoleOutput, "Started executor");

        uint32_t runnableCount = 0;
        while (true) {
            int error = pthread_mutex_lock(&queueMutex);
            if (error != 0) {
                throw runtime_error("Executor::thread_routine pthread_mutex_lock initial failed [error="
                                    + to_string(error)
                                    + "]");
            }
            if (shutDown) {
                CO_IFDEBUG(consoleOutput, "Shutdown, exiting...");
                error = pthread_mutex_unlock(&queueMutex);
                if (error != 0) {
                    throw runtime_error("Executor::thread_routine pthread_mutex_unlock initial shutdown failed [error="
                                        + to_string(error)
                                        + "]");
                }
                break;
            }
            //If queue is empty, wait for data
            if (used == 0) {
                timespec timeToWait;
                error = clock_gettime(CLOCK_MONOTONIC_COARSE, &timeToWait);
                if (error != 0) {
                    pthread_mutex_unlock(&queueMutex);
                    throw runtime_error("Executor::thread_routine clock_gettime failed [error="
                                        + to_string(error)
                                        + ", errno="
                                        + to_string(errno)
                                        + ", strerror="
                                        + strerror(errno)
                                        + "]");
                }
                timeToWait.tv_sec += queueWait.tv_sec;
                timeToWait.tv_nsec += queueWait.tv_nsec;
                error = pthread_cond_timedwait(&queueAddedCond,
                                               &queueMutex,
                                               &timeToWait);
                if (error != 0) {
                    if (error == ETIMEDOUT) {
                        if (shutDown) {
                            CO_IFDEBUG(consoleOutput,
                                       "Shutdown after timeout, exiting...");
                            error = pthread_mutex_unlock(&queueMutex);
                            if (error != 0) {
                                throw runtime_error("Executor::thread_routine pthread_mutex_unlock after timeout shutdown failed [error="
                                                    + to_string(error)
                                                    + "]");
                            }
                            break;
                        }
                        CO_IFDEBUG(consoleOutput, "Timeout, continuing...");
                        error = pthread_mutex_unlock(&queueMutex);
                        if (error != 0) {
                            throw runtime_error("Executor::thread_routine pthread_mutex_unlock after timeout continue failed [error="
                                                + to_string(error)
                                                + "]");
                        }
                        continue;
                    }
                    else {
                        pthread_mutex_unlock(&queueMutex);
                        throw runtime_error("Executor::thread_routine pthread_cond_timedwait initial shutdown failed [error="
                                            + to_string(error)
                                            + "]");
                    }
                }
                //If we have shut down while waiting, stop
                if (shutDown) {
                    CO_IFDEBUG(consoleOutput,
                               "Shutdown after waiting, exiting...");
                    error = pthread_mutex_unlock(&queueMutex);
                    if (error != 0) {
                        throw runtime_error("Executor::thread_routine pthread_mutex_unlock after wait shutdown failed [error="
                                            + to_string(error)
                                            + "]");
                    }
                    break;
                }
                //If we still have nothing to read, loop so we can wait again
                if (used == 0) {
                    error = pthread_mutex_unlock(&queueMutex);
                    if (error != 0) {
                        throw runtime_error("Executor::thread_routine pthread_mutex_unlock after wait empty failed [error="
                                            + to_string(error)
                                            + "]");
                    }
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
            CO_IFDEBUG(consoleOutput,
                       "Executing runnable [runnableCount="<<runnableCount<<", toRun="<<*toRun<<"]");
            //Unlock and signal so that others can also process
            error = pthread_mutex_unlock(&queueMutex);
            if (error != 0) {
                throw runtime_error("Executor::thread_routine pthread_mutex_unlock before job run failed [error="
                                    + to_string(error)
                                    + "]");
            }
            error = pthread_cond_signal(&queueRemovedCond);
            if (error != 0) {
                throw runtime_error("Executor::thread_routine pthread_cond_signal failed [error="
                                    + to_string(error)
                                    + "]");
            }
            //Run the runnable
            try {
                toRun->run();
                runnableCount++;
            }
            catch (const exception& ex) {
                consoleOutput.errorOutput() << "Run error occurred: [runnableCount="
                                            << runnableCount
                                            << ", ex.what="
                                            << ex.what()
                                            << "]"
                                            << endl;
                doShutdown();
                break;
            }
            catch (...) {
                consoleOutput.errorOutput() << "Unknown run failure occurred [runnableCount="
                                            << runnableCount
                                            << "]"
                                            << endl;
                doShutdown();
                break;
            }
        }
    }
    catch (const exception& ex) {
        consoleOutput.errorOutput() << "Executor error occurred: "
                                    << ex.what()
                                    << endl;
        shutDown = true;
    }
    catch (...) {
        consoleOutput.errorOutput() << "Unknown executor failure occurred"
                                    << endl;
        shutDown = true;
    }
    pthread_exit(nullptr);
    return nullptr;
}

void Executor::addToQueue(Runnable* toAdd) {
    if (toAdd == nullptr) {
        throw runtime_error("Can't add null runnable to queue");
    }
    while (true) {
        int error = pthread_mutex_lock(&queueMutex);
        if (error != 0) {
            throw runtime_error("Executor::addToQueue pthread_mutex_lock initial failed [error="
                                + to_string(error)
                                + "]");
        }
        if (shutDown) {
            error = pthread_mutex_unlock(&queueMutex);
            if (error != 0) {
                throw runtime_error("Executor::addToQueue pthread_mutex_unlock initial shutdown failed [error="
                                    + to_string(error)
                                    + "]");
            }
            throw runtime_error("Executor shutdown, can't add runnable (Before wait)");
        }
        //If queue is full, wait for someone to remove things
        if (used == queueSize) {
            timespec timeToWait;
            error = clock_gettime(CLOCK_MONOTONIC_COARSE, &timeToWait);
            if (error != 0) {
                pthread_mutex_unlock(&queueMutex);
                throw runtime_error("Executor::addToQueue clock_gettime failed [error="
                                    + to_string(error)
                                    + ", errno="
                                    + to_string(errno)
                                    + ", strerror="
                                    + strerror(errno)
                                    + "]");
            }
            timeToWait.tv_sec += queueWait.tv_sec;
            timeToWait.tv_nsec += queueWait.tv_nsec;
            error = pthread_cond_timedwait(&queueRemovedCond,
                                           &queueMutex,
                                           &timeToWait);
            if (error != 0) {
                if (error == ETIMEDOUT) {
                    if (shutDown) {
                        error = pthread_mutex_unlock(&queueMutex);
                        if (error != 0) {
                            throw runtime_error("Executor::addToQueue pthread_mutex_unlock after timeout shutdown failed [error="
                                                + to_string(error)
                                                + "]");
                        }
                        throw runtime_error("Executor shutdown, can't add runnable (Wait timeout)");
                    }
                    error = pthread_mutex_unlock(&queueMutex);
                    if (error != 0) {
                        throw runtime_error("Executor::addToQueue pthread_mutex_unlock after timeout continue failed [error="
                                            + to_string(error)
                                            + "]");
                    }
                    continue;
                }
                else {
                    pthread_mutex_unlock(&queueMutex);
                    throw runtime_error("Got error while attempting to wait, can't add runnable [error="
                                        + to_string(error)
                                        + "]");
                }
            }
            //If we have shut down while we were waiting
            if (shutDown) {
                error = pthread_mutex_unlock(&queueMutex);
                if (error != 0) {
                    throw runtime_error("Executor::addToQueue pthread_mutex_unlock after wait shutdown failed [error="
                                        + to_string(error)
                                        + "]");
                }
                throw runtime_error("Executor shutdown, can't add runnable (After wait)");
            }
            //If the queue is still full, loop so we can wait again
            if (used == queueSize) {
                error = pthread_mutex_unlock(&queueMutex);
                if (error != 0) {
                    throw runtime_error("Executor::addToQueue pthread_mutex_unlock after wait full failed [error="
                                        + to_string(error)
                                        + "]");
                }
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
    int error = pthread_mutex_unlock(&queueMutex);
    if (error != 0) {
        throw runtime_error("Executor::addToQueue pthread_mutex_unlock after wait shutdown failed [error="
                            + to_string(error)
                            + "]");
    }
    error = pthread_cond_signal(&queueAddedCond);
    if (error != 0) {
        throw runtime_error("Executor::addToQueue pthread_cond_signal failed [error="
                            + to_string(error)
                            + "]");
    }
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
       << ", queueWait.tv_sec="
       << toPrint.queueWait.tv_sec
       << ", queueWait.tv_nsec="
       << toPrint.queueWait.tv_nsec
       << ", shutDown="
       << toPrint.shutDown
       << ", queueStart="
       << toPrint.queueStart
       << ", queueEnd="
       << toPrint.queueEnd
       << ", used="
       << toPrint.used
       << "]";
    return os;
}
