/*
 * HashFunction.cpp
 *
 *  Created on: 9 Δεκ 2018
 *      Author: parisbre56
 */

#include "HashFunction.h"

using namespace std;

HashFunction::HashFunction() {
    //Nothing
}

HashFunction::~HashFunction() {
    //Nothing
}

ostream& operator<<(ostream& os, const HashFunction& toPrint) {
    toPrint.write(os);
    return os;
}
