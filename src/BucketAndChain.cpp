/*
 * BucketAndChain.cpp
 *
 *  Created on: 23 Οκτ 2018
 *      Author: pibre
 */

#include "BucketAndChain.h"

using namespace std;

BucketAndChain::BucketAndChain(const HashTable& hashTable,
                               uint32_t hashBucket,
                               uint32_t subBuckets,
                               uint32_t (* const hashFunction)(uint32_t,
                                                               int32_t),
                               const ConsoleOutput * const consoleOutput) :
        referenceTable(hashTable.getBucket(hashBucket)),
        tuplesInBucket(hashTable.getTuplesInBucket(hashBucket)),
        subBuckets(subBuckets),
        hashFunction(hashFunction),
        consoleOutput(consoleOutput),
        bucket(new uint32_t[subBuckets]),
        chain(new uint32_t[hashTable.getTuplesInBucket(hashBucket)] { }) {
    debug("Splitting "
          + to_string(tuplesInBucket)
          + " tuples to "
          + to_string(subBuckets)
          + " subBuckets");

    //Initialize tables to tuplesInBucket (that way we can tell when a position is unset)
    debug("Initializing bucket");
    for (uint32_t i = 0; i < subBuckets; ++i) {
        bucket[i] = tuplesInBucket;
    }
    debug("Bucket initialized");

    debug("Starting split");
    for (uint32_t i = 0; i < tuplesInBucket; ++i) {
        debug("Processing tuple " + to_string(i));
        const Tuple& currTuple = *(referenceTable[i]);
        debug(to_string(i) + ":" + currTuple.toString());

        uint32_t currHash = this->hashFunction(this->subBuckets,
                                               currTuple.getPayload());
        debug("Assigned to subBucket " + to_string(currHash));

        //If this is the first time the bucket is used, then the end of the chain will
        //be initialized to tuplesInBucket
        chain[i] = bucket[currHash];
        bucket[currHash] = i;

        debug("chain[i]=" + to_string(chain[i]));
        debug("bucket[currHash]=" + to_string(bucket[currHash]));
    }
    debug("Split complete");

}

void BucketAndChain::join(HashTable& hashToJoin,
                          uint32_t bucketToJoin,
                          Result& resultAggregator) {
    debug("Joining with bucket "
          + to_string(bucketToJoin)
          + " of given hashTable");
    const Tuple* const * tuplesToJoin = hashToJoin.getBucket(bucketToJoin);
    uint32_t numTuplesToJoin = hashToJoin.getTuplesInBucket(bucketToJoin);

    debug("Examining "
          + to_string(numTuplesToJoin)
          + " tuples in given hashTable");
    for (uint32_t i = 0; i < numTuplesToJoin; ++i) {
        debug("Examining tuple " + i);
        const Tuple& currTuple = *(tuplesToJoin[i]);
        debug("o" + to_string(i) + ":" + currTuple.toString());

        uint32_t currHash = this->hashFunction(subBuckets,
                                               currTuple.getPayload());
        debug("Searching subBucket " + to_string(currHash));

        uint32_t searchPoint = bucket[currHash];
        debug("Searching chain with start point " + to_string(searchPoint));
        while (searchPoint != tuplesInBucket) {
            debug("Searching chain point " + to_string(searchPoint));
            const Tuple& searchPointTuple = *(referenceTable[searchPoint]);
            debug("c"
                  + to_string(searchPoint)
                  + ":"
                  + searchPointTuple.toString());
            if (searchPointTuple.getPayload() == currTuple.getPayload()) {
                Tuple joinRow(searchPointTuple.getKey(), currTuple.getKey());
                debug("Adding joinRow " + joinRow.toString());
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

void BucketAndChain::debug(string outString) {
    if (consoleOutput != nullptr) {
        consoleOutput->debugOutput(outString);
    }
}

void BucketAndChain::error(string outString) {
    if (consoleOutput != nullptr) {
        consoleOutput->errorOutput(outString);
    }
}
