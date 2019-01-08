/*
 * HashFunctionModulo.cpp
 *
 *  Created on: 9 Δεκ 2018
 *      Author: parisbre56
 */

#include "HashFunctionModulo.h"

using namespace std;

HashFunctionModulo::HashFunctionModulo(uint32_t buckets):HashFunction(),buckets(buckets < HASHFUNCTIONMODULO_H_MIN_BUCKETS?HASHFUNCTIONMODULO_H_MIN_BUCKETS:buckets) {

}

HashFunctionModulo::~HashFunctionModulo() {
    //Nothing
}

void HashFunctionModulo::write(std::ostream& os) const {
    os << "[HashFunctionModulo buckets=" << buckets << "]";
}
uint32_t HashFunctionModulo::applyHash(uint64_t value) const {
    value ^= (value << 17) | (value >> 16);
    return value % buckets;
}
uint32_t HashFunctionModulo::getBuckets() const {
    return buckets;
}

ostream& operator<<(ostream& os,
        const HashFunctionModulo& toPrint) {
    toPrint.write(os);
    return os;
}
