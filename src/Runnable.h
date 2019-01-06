/*
 * Runnable.h
 *
 *  Created on: 4 Ιαν 2019
 *      Author: pibre
 */

#ifndef RUNNABLE_H_
#define RUNNABLE_H_
class Runnable;

#include <ostream>

class Runnable {
protected:
    virtual void printSelf(std::ostream& os) const;
public:
    Runnable();
    virtual ~Runnable();

    virtual void run() = 0;

    friend std::ostream& operator<<(std::ostream& os, const Runnable& toPrint);
};

#endif /* RUNNABLE_H_ */
