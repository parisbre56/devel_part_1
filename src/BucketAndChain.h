/*
 * BucketAndChain.h
 *
 *  Created on: 23 Ξ�ΞΊΟ„ 2018
 *      Author: pibre
 */

#ifndef BUCKETANDCHAIN_H_
#define BUCKETANDCHAIN_H_
class BucketAndChain;

#include <cstdint>
#include <string>

#include "HashTable.h"
#include "ResultContainer.h"
#include "HashFunction.h"

class BucketAndChain {
protected:
    const Tuple * const * const referenceTable;
    /** tuplesInBucket's value is used to show the end of a chain or a bucket with no values. **/
    const uint64_t tuplesInBucket;
    const uint32_t subBuckets;
    HashFunction& hashFunction;
    uint64_t * const bucket;
    uint64_t * const chain;
    const uint32_t sizeTableRows;
    const size_t sizePayloads;
    const bool * const usedRows; //Not managed by BucketAndChain, will not be deleted
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
                   const uint32_t hashBucket,
                   HashFunction& hashFunction);
    virtual ~BucketAndChain();

    void join(HashTable& hashToJoin,
              uint32_t bucketToJoin,
              ResultContainer& resultAggregator);

    friend std::ostream& operator<<(std::ostream& os,
                                    const BucketAndChain& toPrint);
};

#endif /* BUCKETANDCHAIN_H_ */
