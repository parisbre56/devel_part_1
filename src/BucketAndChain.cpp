/*
 * BucketAndChain.cpp
 *
 *  Created on: 23 Οκτ 2018
 *      Author: pibre
 */

#include "BucketAndChain.h"

#include <sstream>

using namespace std;

BucketAndChain::BucketAndChain(const HashTable& hashTable,
                               uint32_t hashBucket,
                               uint32_t subBuckets,
                               uint32_t (* const hashFunction)(uint32_t,
                                                               int32_t)) :
        referenceTable(hashTable.getBucket(hashBucket)),
        tuplesInBucket(hashTable.getTuplesInBucket(hashBucket)),
        subBuckets(subBuckets),
        hashFunction(hashFunction),
        bucket(new uint32_t[subBuckets]),
        chain(new uint32_t[hashTable.getTuplesInBucket(hashBucket)] { }) {
    ConsoleOutput consoleOutput("BucketAndChain");
    CO_IFDEBUG(consoleOutput,
               "Splitting " << tuplesInBucket << " tuples to " << this->subBuckets << " subBuckets");

    //Initialize tables to tuplesInBucket (that way we can tell when a position is unset)
    CO_IFDEBUG(consoleOutput, "Initializing bucket");
    for (uint32_t i = 0; i < this->subBuckets; ++i) {
        bucket[i] = tuplesInBucket;
    }
    CO_IFDEBUG(consoleOutput, "Bucket initialized");

    CO_IFDEBUG(consoleOutput, "Starting split");
    for (uint32_t i = 0; i < tuplesInBucket; ++i) {
        CO_IFDEBUG(consoleOutput, "Processing tuple " << i);
        const Tuple& currTuple = *(referenceTable[i]);
        CO_IFDEBUG(consoleOutput, i << ":" << currTuple);

        uint32_t currHash = this->hashFunction(this->subBuckets,
                                               currTuple.getPayload());
        CO_IFDEBUG(consoleOutput, "Assigned to subBucket " << currHash);

        //If this is the first time the bucket is used, then the end of the chain will
        //be initialized to tuplesInBucket
        chain[i] = bucket[currHash];
        bucket[currHash] = i;

        CO_IFDEBUG(consoleOutput, "chain[i]=" << chain[i]);
        CO_IFDEBUG(consoleOutput, "bucket[currHash]=" << bucket[currHash]);
    }
    CO_IFDEBUG(consoleOutput, "Split complete");

}

void BucketAndChain::join(HashTable& hashToJoin,
                          uint32_t bucketToJoin,
                          Result& resultAggregator) {
    ConsoleOutput consoleOutput("BucketAndChain::join");
    CO_IFDEBUG(consoleOutput,
               "Joining with bucket " << bucketToJoin << " of given hashTable");
    const Tuple* const * tuplesToJoin = hashToJoin.getBucket(bucketToJoin);
    uint32_t numTuplesToJoin = hashToJoin.getTuplesInBucket(bucketToJoin);

    CO_IFDEBUG(consoleOutput,
               "Examining " << numTuplesToJoin << " tuples in given hashTable");
    for (uint32_t i = 0; i < numTuplesToJoin; ++i) {
        CO_IFDEBUG(consoleOutput, "Examining tuple " << i);
        const Tuple& currTuple = *(tuplesToJoin[i]);
        CO_IFDEBUG(consoleOutput, "o" << i << ":" << currTuple);

        uint32_t currHash = this->hashFunction(subBuckets,
                                               currTuple.getPayload());
        CO_IFDEBUG(consoleOutput, "Searching subBucket " << currHash);

        uint32_t searchPoint = bucket[currHash];
        CO_IFDEBUG(consoleOutput,
                   "Searching chain with start point " << searchPoint);
        while (searchPoint != tuplesInBucket) {
            CO_IFDEBUG(consoleOutput, "Searching chain point " << searchPoint);
            const Tuple& searchPointTuple = *(referenceTable[searchPoint]);
            CO_IFDEBUG(consoleOutput,
                       "c" << searchPoint << ":" << searchPointTuple);
            if (searchPointTuple.getPayload() == currTuple.getPayload()) {
                Tuple joinRow(searchPointTuple.getKey(), currTuple.getKey());
                CO_IFDEBUG(consoleOutput, "Adding joinRow " << joinRow);
                resultAggregator.addTuple(joinRow);
            }
            searchPoint = chain[searchPoint];
        }
    }
}

BucketAndChain::~BucketAndChain() {
    delete[] bucket;
    delete[] chain;
}

std::ostream& operator<<(std::ostream& os, const BucketAndChain& toPrint) {
    os << "[BucketAndChain tuplesInBucket="
       << toPrint.tuplesInBucket
       << ", subBuckets="
       << toPrint.subBuckets
       << ", hashFunction="
       << ((void*) toPrint.hashFunction)
       << ", bucket=[";
    for (uint32_t i = 0; i < toPrint.subBuckets; ++i) {
        if (i != 0) {
            os << ", ";
        }
        os << toPrint.bucket[i];
    }
    os << "], i:chain[i]:referenceTable[i]=[";
    for (uint32_t i = 0; i < toPrint.tuplesInBucket; ++i) {
        os << "\n\t"
           << i
           << ":\t"
           << toPrint.chain[i]
           << ":\t"
           << (*(toPrint.referenceTable[i]));
    }
    os << "]]";
    return os;
}

