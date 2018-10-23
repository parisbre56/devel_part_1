/*
 * Tuple.cpp
 *
 *  Created on: 21 ��� 2018
 *      Author: parisbre56
 */

#include "Tuple.h"

#include <sstream>

using namespace std;

Tuple::Tuple() {
    key = 0;
    payload = 0;
}

Tuple::Tuple(int32_t key, int32_t payload) {
    this->key = key;
    this->payload = payload;
}

Tuple::Tuple(const Tuple& toCopy) :
        Tuple(toCopy.key, toCopy.payload) {
}

Tuple& Tuple::operator=(const Tuple& toCopy) {
    this->key = toCopy.key;
    this->payload = toCopy.payload;
    return *this;
}

Tuple::~Tuple() {
    // TODO Auto-generated destructor stub
}

int32_t Tuple::getKey() const {
    return key;
}

void Tuple::setKey(int32_t key) {
    this->key = key;
}

int32_t Tuple::getPayload() const {
    return payload;
}

void Tuple::setPayload(int32_t payload) {
    this->payload = payload;
}

string Tuple::toString() const {
    ostringstream retVal;
    retVal << "[Tuple key=" << key << ", payload=" << payload << "]";
    return retVal.str();
}
