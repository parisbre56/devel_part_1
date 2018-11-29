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

Tuple::Tuple(uint64_t key, uint64_t payload) {
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
    //Do nothing
}

uint64_t Tuple::getKey() const {
    return key;
}

void Tuple::setKey(uint64_t key) {
    this->key = key;
}

uint64_t Tuple::getPayload() const {
    return payload;
}

void Tuple::setPayload(uint64_t payload) {
    this->payload = payload;
}

std::ostream& operator<<(std::ostream& os, const Tuple& toPrint) {
    os << "[Tuple key="
       << toPrint.key
       << ", payload="
       << toPrint.payload
       << "]";
    return os;
}
