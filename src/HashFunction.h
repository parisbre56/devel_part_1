/*
 * HashFunction.h
 *
 *  Created on: 9 Δεκ 2018
 *      Author: parisbre56
 */

#ifndef HASHFUNCTION_H_
#define HASHFUNCTION_H_
class HashFunction;

#include <string>

#include <cstdint>

class HashFunction {
protected:
    virtual void write(std::ostream& os) const = 0;
public:
    HashFunction();
    HashFunction(const HashFunction& toCopy) = delete;
    HashFunction(HashFunction&& toMove) = delete;
    HashFunction& operator=(const HashFunction& toCopy) = delete;
    HashFunction& operator=(HashFunction&& toMove) = delete;
    virtual ~HashFunction();

    virtual uint32_t applyHash(uint64_t value) const = 0;
    virtual uint32_t getBuckets() const = 0;

    friend std::ostream& operator<<(std::ostream& os,
                                    const HashFunction& toPrint);
};

#endif /* HASHFUNCTION_H_ */
