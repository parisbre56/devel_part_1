/*
 * BucketAndChain.h
 *
 *  Created on: 23 Ξ�ΞΊΟ„ 2018
 *      Author: pibre
 */

#ifndef BUCKETANDCHAIN_H_
#define BUCKETANDCHAIN_H_

#include <cstdint>
#include <string>

#include "HashTable.h"
#include "ResultContainer.h"

class BucketAndChain {
protected:
    const Tuple * const referenceTable;
    /** tuplesInBucket's value is used to show the end of a chain or a bucket with no values. **/
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
              ResultContainer& resultAggregator);

    friend std::ostream& operator<<(std::ostream& os,
                                    const BucketAndChain& toPrint);
};

#endif /* BUCKETANDCHAIN_H_ */
