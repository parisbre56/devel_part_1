/*
 * HashTable.h
 *
 *  Created on: 22 ��� 2018
 *      Author: pibre
 */

#ifndef HASHTABLE_H_
#define HASHTABLE_H_
class HashTable;

#define HASHTABLE_H_SEGMENT_SIZE 10000

#include <string>

#include <cstdint>

#include "ConsoleOutput.h"
#include "Relation.h"
#include "HashFunction.h"
#include "Executor.h"
#include "HistogramJob.h"
#include "PartitionJob.h"

class HashTable {
protected:
    Executor& executor;
    const uint32_t buckets;
    const uint64_t numTuples;
    const HashFunction& hashFunction;
    uint64_t * const histogram;
    uint64_t * const pSum;
    const uint32_t sizeTableRows;
    const size_t sizePayloads;
    const bool * const usedRows; //Not managed by HashTable, will not be deleted
    const Tuple * * const orderedTuples;
    uint64_t segments;
    HistogramJob * * histogramJobs;
    PartitionJob * * partitionJobs;
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
    HashTable(Executor& executor,
              const Relation& relation,
              const HashFunction& hashFunction);

    HashTable(const HashTable& toCopy) = delete;
    HashTable& operator=(const HashTable& toCopy) = delete;
    virtual ~HashTable();

    uint32_t getBuckets() const;
    uint64_t getTuplesInBucket(uint32_t bucket) const;
    const Tuple * const * getBucket(uint32_t bucket) const;
    /** Get the (index) element of the (bucket) bucket **/
    const Tuple& getTuple(uint32_t bucket, uint64_t index) const;
    const bool* getUsedRows() const;

    size_t getSizePayloads() const;
    uint32_t getSizeTableRows() const;
    const uint64_t getNumTuples() const;

    friend std::ostream& operator<<(std::ostream& os, const HashTable& toPrint);
};

#endif /* HASHTABLE_H_ */
