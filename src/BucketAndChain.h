/*
 * BucketAndChain.h
 *
 *  Created on: 23 Οκτ 2018
 *      Author: pibre
 */

#ifndef BUCKETANDCHAIN_H_
#define BUCKETANDCHAIN_H_

#include <cstdint>
#include <string>

#include "HashTable.h"
#include "Result.h"

class BucketAndChain {
protected:
    const Tuple * const * const referenceTable;
    const uint32_t tuplesInBucket;
    const uint32_t subBuckets;
    uint32_t (* const hashFunction)(uint32_t, int32_t);
    const ConsoleOutput * const consoleOutput;
    uint32_t * const bucket;
    uint32_t * const chain;

    void debug(std::string outString);
    void error(std::string outString);
public:
    BucketAndChain() = delete;
    BucketAndChain(const BucketAndChain& toCopy) = delete;
    BucketAndChain& operator=(const BucketAndChain& toCopy) = delete;
    BucketAndChain(const HashTable& hashTable,
                   uint32_t bucket,
                   uint32_t subBuckets,
                   uint32_t (* const hashFunction)(uint32_t, int32_t),
                   const ConsoleOutput * const consoleOutput = nullptr);
    virtual ~BucketAndChain();

    void join(HashTable& hashToJoin,
              uint32_t bucketToJoin,
              Result& resultAggregator);

    std::string toString();
};

#endif /* BUCKETANDCHAIN_H_ */
