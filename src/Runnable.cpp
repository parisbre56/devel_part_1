/*
 * Runnable.cpp
 *
 *  Created on: 4 Ιαν 2019
 *      Author: pibre
 */

#include "Runnable.h"

using namespace std;

Runnable::Runnable() {
    //Do nothing
}

Runnable::~Runnable() {
    //Do nothing
}

void Runnable::printSelf(ostream& os) const {
    os << "[Runnable]";
}

ostream& operator<<(ostream& os, const Runnable& toPrint) {
    toPrint.printSelf(os);
    return os;
}
