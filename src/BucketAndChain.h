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
    uint32_t * const bucket;
    uint32_t * const chain;
public:
    BucketAndChain() = delete;
    BucketAndChain(const BucketAndChain& toCopy) = delete;
    BucketAndChain& operator=(const BucketAndChain& toCopy) = delete;
    /** Create a BucketAndChain subHashTable from the given bucket of the given hashTable.
     *
     * SubBuckets should be equal to the number of buckets returned by the hash function,
     * though it can also work if it's greater.
     *
     * Note that the data of hashTable are NOT copied, so the hashTable should NOT
     * be deleted before the BucketAndChain is. **/
    BucketAndChain(const HashTable& hashTable,
                   uint32_t bucket,
                   uint32_t subBuckets,
                   uint32_t (* const hashFunction)(uint32_t, int32_t));
    virtual ~BucketAndChain();

    void join(HashTable& hashToJoin,
              uint32_t bucketToJoin,
              Result& resultAggregator);

    std::string toString();
};

#endif /* BUCKETANDCHAIN_H_ */
