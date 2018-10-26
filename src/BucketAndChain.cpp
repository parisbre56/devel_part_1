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
               "Splitting "
               + to_string(tuplesInBucket)
               + " tuples to "
               + to_string(this->subBuckets)
               + " subBuckets");

    //Initialize tables to tuplesInBucket (that way we can tell when a position is unset)
    CO_IFDEBUG(consoleOutput, "Initializing bucket");
    for (uint32_t i = 0; i < this->subBuckets; ++i) {
        bucket[i] = tuplesInBucket;
    }
    CO_IFDEBUG(consoleOutput, "Bucket initialized");

    CO_IFDEBUG(consoleOutput, "Starting split");
    for (uint32_t i = 0; i < tuplesInBucket; ++i) {
        CO_IFDEBUG(consoleOutput, "Processing tuple " + to_string(i));
        const Tuple& currTuple = *(referenceTable[i]);
        CO_IFDEBUG(consoleOutput, to_string(i) + ":" + currTuple.toString());

        uint32_t currHash = this->hashFunction(this->subBuckets,
                                               currTuple.getPayload());
        CO_IFDEBUG(consoleOutput,
                   "Assigned to subBucket " + to_string(currHash));

        //If this is the first time the bucket is used, then the end of the chain will
        //be initialized to tuplesInBucket
        chain[i] = bucket[currHash];
        bucket[currHash] = i;

        CO_IFDEBUG(consoleOutput, "chain[i]=" + to_string(chain[i]));
        CO_IFDEBUG(consoleOutput,
                   "bucket[currHash]=" + to_string(bucket[currHash]));
    }
    CO_IFDEBUG(consoleOutput, "Split complete");

}

void BucketAndChain::join(HashTable& hashToJoin,
                          uint32_t bucketToJoin,
                          Result& resultAggregator) {
    ConsoleOutput consoleOutput("JOIN");
    CO_IFDEBUG(consoleOutput,
               "Joining with bucket "
               + to_string(bucketToJoin)
               + " of given hashTable");
    const Tuple* const * tuplesToJoin = hashToJoin.getBucket(bucketToJoin);
    uint32_t numTuplesToJoin = hashToJoin.getTuplesInBucket(bucketToJoin);

    CO_IFDEBUG(consoleOutput,
               "Examining "
               + to_string(numTuplesToJoin)
               + " tuples in given hashTable");
    for (uint32_t i = 0; i < numTuplesToJoin; ++i) {
        CO_IFDEBUG(consoleOutput, "Examining tuple " + i);
        const Tuple& currTuple = *(tuplesToJoin[i]);
        CO_IFDEBUG(consoleOutput,
                   "o" + to_string(i) + ":" + currTuple.toString());

        uint32_t currHash = this->hashFunction(subBuckets,
                                               currTuple.getPayload());
        CO_IFDEBUG(consoleOutput, "Searching subBucket " + to_string(currHash));

        uint32_t searchPoint = bucket[currHash];
        CO_IFDEBUG(consoleOutput,
                   "Searching chain with start point " + to_string(searchPoint));
        while (searchPoint != tuplesInBucket) {
            CO_IFDEBUG(consoleOutput,
                       "Searching chain point " + to_string(searchPoint));
            const Tuple& searchPointTuple = *(referenceTable[searchPoint]);
            CO_IFDEBUG(consoleOutput,
                       "c"
                       + to_string(searchPoint)
                       + ":"
                       + searchPointTuple.toString());
            if (searchPointTuple.getPayload() == currTuple.getPayload()) {
                Tuple joinRow(searchPointTuple.getKey(), currTuple.getKey());
                CO_IFDEBUG(consoleOutput,
                           "Adding joinRow " + joinRow.toString());
                resultAggregator.addTuple(joinRow);
            }
            searchPoint = chain[searchPoint];
        }
    }
}

BucketAndChain::~BucketAndChain() {
    delete bucket;
    delete chain;
}

string BucketAndChain::toString() {
    ostringstream retVal;
    retVal << "[BucketAndChain tuplesInBucket="
           << to_string(tuplesInBucket)
           << ", subBuckets="
           << to_string(subBuckets)
           << ", hashFunction="
           << (void*) hashFunction
           << ", bucket=[";
    for (uint32_t i = 0; i < subBuckets; ++i) {
        if (i != 0) {
            retVal << ", ";
        }
        retVal << to_string(bucket[i]);
    }
    retVal << "], chain:referenceTable=[";
    for (uint32_t i = 0; i < tuplesInBucket; ++i) {
        retVal << "\n\t"
               << to_string(i)
               << ":\t"
               << to_string(chain[i])
               << ":\t"
               << referenceTable[i]->toString();
    }
    retVal << "]]";
    return retVal.str();
}
