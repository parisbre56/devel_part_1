/*
 * HashFunctionBitmask.h
 *
 *  Created on: 9 Δεκ 2018
 *      Author: parisbre56
 */

#ifndef HASHFUNCTIONBITMASK_H_
#define HASHFUNCTIONBITMASK_H_

#include "HashFunction.h"

#include <string>

#define HASHFUNCTIONBITMASK_H_MIN_HASH_BITS 1

class HashFunctionBitmask: public HashFunction {
protected:
    const unsigned char bitmaskSize;
    const uint32_t buckets;
    const uint32_t bitmask;

    virtual void write(std::ostream& os) const;
public:
    HashFunctionBitmask() = delete;
    HashFunctionBitmask(unsigned char bitmaskSize);
    HashFunctionBitmask(const HashFunctionBitmask& toCopy) = delete;
    HashFunctionBitmask(HashFunctionBitmask&& toMove) = delete;
    HashFunctionBitmask& operator=(const HashFunctionBitmask& toCopy) = delete;
    HashFunctionBitmask& operator=(HashFunctionBitmask&& toMove) = delete;
    virtual ~HashFunctionBitmask();

    uint32_t applyHash(uint64_t value) const;
    uint32_t getBuckets() const;

    friend std::ostream& operator<<(std::ostream& os,
                                    const HashFunctionBitmask& toPrint);
};

#endif /* HASHFUNCTIONBITMASK_H_ */
