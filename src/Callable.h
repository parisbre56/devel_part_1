/*
 * Callable.h
 *
 *  Created on: 4 Ιαν 2019
 *      Author: pibre
 */

#ifndef CALLABLE_H_
#define CALLABLE_H_
template<class T>
class Callable;

#include <ostream>

#include <pthread.h>

#include "Runnable.h"

template<class T>
class Callable: Runnable {
protected:
    pthread_mutex_t finishedMutex;
    pthread_cond_t finishedCond;
    bool finished;

    virtual void printSelf(std::ostream& os);
    virtual T* getResultInternal() = 0;
    virtual void runInternal() = 0;
public:
    Callable();
    Callable(const Callable& toCopy) = delete;
    Callable(Callable&& toMove) = delete;
    Callable& operator=(const Callable& toCopy) = delete;
    Callable& operator=(Callable&& toMove) = delete;
    virtual ~Callable();

    void run() final;
    bool getFinished();
    T* waitAndGetResult();

    template<class F>
    friend std::ostream& operator<<(std::ostream& os,
                                    const Callable<F>& toPrint);
};

#endif /* CALLABLE_H_ */
