/*
 * Runnable.h
 *
 *  Created on: 4 Ιαν 2019
 *      Author: pibre
 */

#ifndef RUNNABLE_H_
#define RUNNABLE_H_

class Runnable {
public:
    Runnable();
    virtual ~Runnable();

    virtual void run() = 0;
};

#endif /* RUNNABLE_H_ */
