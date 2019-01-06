/*
 * Callable.cpp
 *
 *  Created on: 4 Ιαν 2019
 *      Author: pibre
 */

#include "Callable.h"

using namespace std;

template<class T>
void Callable<T>::printSelf(ostream& os) const {
    os << "[Callable finished=" << finished << "]";
}

template<class T>
Callable<T>::Callable() :
        finishedMutex(PTHREAD_MUTEX_INITIALIZER),
        finishedCond(PTHREAD_COND_INITIALIZER),
        finished(false) {
}

template<class T>
Callable<T>::~Callable() {
    pthread_mutex_destroy(&finishedMutex);
    pthread_cond_destroy(&finishedCond);
}

template<class T>
void Callable<T>::run() {
    runInternal();
    pthread_mutex_lock(&finishedMutex);
    finished = true;
    pthread_mutex_unlock(&finishedMutex);
    pthread_cond_broadcast(&finishedCond);
}

template<class T>
bool Callable<T>::getFinished() {
    pthread_mutex_lock(&finishedMutex);
    bool retVal = finished;
    pthread_mutex_unlock(&finishedMutex);
    return retVal;
}

template<class T>
T* Callable<T>::waitAndGetResult() {
    pthread_mutex_lock(&finishedMutex);
    while (!finished) {
        pthread_cond_wait(&finishedCond, &finishedMutex);
    }
    pthread_mutex_unlock(&finishedMutex);
    return getResultInternal();
}

template<class T>
ostream& operator<<(ostream& os, const Callable<T>& toPrint) {
    toPrint.printSelf(os);
    return os;
}
