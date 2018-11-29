/*
 * HashTable.h
 *
 *  Created on: 22 ��� 2018
 *      Author: pibre
 */

#ifndef HASHTABLE_H_
#define HASHTABLE_H_

#include <string>

#include <cstdint>

#include "ConsoleOutput.h"
#include "Relation.h"

class HashTable {
protected:
    const uint32_t buckets;
    const uint64_t numTuples;
    uint32_t (* const hashFunction)(uint32_t, uint64_t);
    uint64_t * const histogram;
    uint64_t * const pSum;
    Tuple * const orderedTuples;
public:
    HashTable() = delete;
    /** Create a hashTable for the given relation, using the given number
     * of buckets and the given hash function.
     *
     * The first parameter to the hashFunction is the number of buckets and
     * the second is the payload. It returns the 0-base bucket to which that
     * payload should be assigned.
     *
     * The console output is optional. If not null, it is used to write debug
     * and error info. **/
    HashTable(const Relation& relation,
              uint32_t buckets,
              uint32_t (* const hashFunction)(uint32_t, uint64_t));

    HashTable(const HashTable& toCopy);
    HashTable& operator=(const HashTable& toCopy) = delete;
    virtual ~HashTable();

    uint32_t getBuckets() const;
    uint64_t getTuplesInBucket(uint32_t bucket) const;
    const Tuple * const getBucket(uint32_t bucket) const;
    /** Get the (index) element of the (bucket) bucket **/
    const Tuple& getTuple(uint32_t bucket, uint64_t index) const;
    friend std::ostream& operator<<(std::ostream& os, const HashTable& toPrint);
};

#endif /* HASHTABLE_H_ */
