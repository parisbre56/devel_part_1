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
    const uint32_t numTuples;
    uint32_t (* const hashFunction)(uint32_t, int32_t);
    const ConsoleOutput * const consoleOutput;
    uint32_t * const histogram;
    uint32_t * const pSum;
    const Tuple ** const orderedTuples;

    void debug(std::string outString);
    void error(std::string outString);
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
              uint32_t (* const hashFunction)(uint32_t, int32_t),
              const ConsoleOutput * const consoleOutput = nullptr);

    HashTable(const HashTable& toCopy);
    HashTable& operator=(const HashTable& toCopy) = delete;
    virtual ~HashTable();

    uint32_t getBuckets() const;
    uint32_t getTuplesInBucket(uint32_t bucket) const;
    const Tuple * const * const getBucket(uint32_t bucket) const;
    /** Get the (index) element of the (bucket) bucket **/
    const Tuple& getTuple(uint32_t bucket, uint32_t index) const;

    std::string toString() const;
};

#endif /* HASHTABLE_H_ */
