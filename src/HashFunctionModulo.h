/*
 * HashFunctionModulo.h
 *
 *  Created on: 9 Δεκ 2018
 *      Author: parisbre56
 */

#ifndef HASHFUNCTIONMODULO_H_
#define HASHFUNCTIONMODULO_H_

#include "HashFunction.h"

#include <ostream>

#define HASHFUNCTIONMODULO_H_MIN_BUCKETS 3

class HashFunctionModulo: public HashFunction {
protected:
    const uint32_t buckets;

    virtual void write(std::ostream& os) const;
public:
    HashFunctionModulo() = delete;
    HashFunctionModulo(uint32_t buckets);
    HashFunctionModulo(const HashFunctionModulo& toCopy) = delete;
    HashFunctionModulo(HashFunctionModulo&& toMove) = delete;
    HashFunctionModulo& operator=(const HashFunctionModulo& toCopy) = delete;
    HashFunctionModulo& operator=(HashFunctionModulo&& toMove) = delete;
    virtual ~HashFunctionModulo();

    uint32_t applyHash(uint64_t value) const;
    uint32_t getBuckets() const;

    friend std::ostream& operator<<(std::ostream& os,
                                    const HashFunctionModulo& toPrint);
};

#endif /* HASHFUNCTIONMODULO_H_ */
