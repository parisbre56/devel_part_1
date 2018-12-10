/*
 * HashFunctionBitmask.cpp
 *
 *  Created on: 9 Δεκ 2018
 *      Author: parisbre56
 */

#include "HashFunctionBitmask.h"

#include <ostream>

using namespace std;

HashFunctionBitmask::HashFunctionBitmask(unsigned char bitmaskSize):
HashFunction(),
bitmaskSize(bitmaskSize < HASHFUNCTIONBITMASK_H_MIN_HASH_BITS ? HASHFUNCTIONBITMASK_H_MIN_HASH_BITS : bitmaskSize),
buckets(1 << (bitmaskSize < HASHFUNCTIONBITMASK_H_MIN_HASH_BITS ? HASHFUNCTIONBITMASK_H_MIN_HASH_BITS : bitmaskSize)),
bitmask((1 << (bitmaskSize < HASHFUNCTIONBITMASK_H_MIN_HASH_BITS ? HASHFUNCTIONBITMASK_H_MIN_HASH_BITS : bitmaskSize)) - 1) {

}

HashFunctionBitmask::~HashFunctionBitmask() {
    //Do nothing
}

void HashFunctionBitmask::write(ostream& os) const {
    os << "[HashFunctionBitmask bitmaskSize=" << to_string(bitmaskSize) << ", buckets=" << buckets << ", bitmask=" << bitmask << "]";
}

uint32_t HashFunctionBitmask::applyHash(uint64_t value) const {
    return bitmask & value;
}
uint32_t HashFunctionBitmask::getBuckets() const {
    return buckets;
}

ostream& operator<<(ostream& os,
        const HashFunctionBitmask& toPrint) {
    toPrint.write(os);
    return os;
}
