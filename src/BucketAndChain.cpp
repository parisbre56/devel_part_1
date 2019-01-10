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
                               const uint32_t hashBucket,
                               HashFunction& hashFunction) :
        referenceTable(hashTable.getBucket(hashBucket)),
        tuplesInBucket(hashTable.getTuplesInBucket(hashBucket)),
        subBuckets(hashFunction.getBuckets()),
        hashFunction(hashFunction),
        bucket(new uint64_t[hashFunction.getBuckets()]),
        chain(new uint64_t[hashTable.getTuplesInBucket(hashBucket)] { }),
        sizeTableRows(hashTable.getSizeTableRows()),
        sizePayloads(hashTable.getSizePayloads()),
        usedRows(hashTable.getUsedRows()) {
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
        const Tuple* const currTuple = referenceTable[i];
        CO_IFDEBUG(consoleOutput, i << ":" << *currTuple);

        uint32_t currHash = hashFunction.applyHash(currTuple->getPayload(0));
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

void BucketAndChain::join(const HashTable& hashToJoin,
                          const uint32_t bucketToJoin,
                          ResultContainer& resultAggregator) const {
    ConsoleOutput consoleOutput("BucketAndChain::join");
    CO_IFDEBUG(consoleOutput,
               "Joining with bucket " << bucketToJoin << " of given hashTable");
    const Tuple * const * const tuplesToJoin = hashToJoin.getBucket(bucketToJoin);
    uint64_t numTuplesToJoin = hashToJoin.getTuplesInBucket(bucketToJoin);

    CO_IFDEBUG(consoleOutput,
               "Examining " << numTuplesToJoin << " tuples in given hashTable");
    Tuple joinRow(sizeTableRows, 0);
    for (uint64_t i = 0; i < numTuplesToJoin; ++i) {
        CO_IFDEBUG(consoleOutput, "Examining tuple " << i);
        const Tuple& currTuple = *(tuplesToJoin[i]);
        CO_IFDEBUG(consoleOutput, "o" << i << ":" << currTuple);

        uint32_t currHash = hashFunction.applyHash(currTuple.getPayload(0));
        CO_IFDEBUG(consoleOutput, "Searching subBucket " << currHash);

        uint64_t searchPoint = bucket[currHash];
        CO_IFDEBUG(consoleOutput,
                   "Searching chain with start point " << searchPoint);
        while (searchPoint != tuplesInBucket) {
            CO_IFDEBUG(consoleOutput, "Searching chain point " << searchPoint);
            const Tuple& searchPointTuple = *(referenceTable[searchPoint]);
            CO_IFDEBUG(consoleOutput,
                       "c" << searchPoint << ":" << searchPointTuple);
            bool matches = true;
            for (size_t j = 0; j < sizePayloads; ++j) {
                if (searchPointTuple.getPayload(j) != currTuple.getPayload(j)) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                CO_IFDEBUG(consoleOutput,
                           "Found matching Tuples [searchPointTuple="<<searchPointTuple<<", currTuple="<<currTuple<<"]");
                const bool* hashToJoinUsedRows = hashToJoin.getUsedRows();
                for (uint32_t i = 0; i < sizeTableRows; ++i) {
                    if (usedRows[i]) {
                        joinRow.setTableRow(i, searchPointTuple.getTableRow(i));
                    }
                    else if (hashToJoinUsedRows[i]) {
                        joinRow.setTableRow(i, currTuple.getTableRow(i));
                    }
                }
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
       << toPrint.hashFunction
       << ", sizeTableRows="
       << toPrint.sizeTableRows
       << ", sizePayloads="
       << toPrint.sizePayloads
       << ", usedRows=[";
    for (uint32_t i = 0; i < toPrint.sizeTableRows; ++i) {
        if (i != 0) {
            os << ", ";
        }
        os << toPrint.usedRows[i];
    }
    os << "], bucket=[";
    for (uint32_t i = 0; i < toPrint.subBuckets; ++i) {
        if (i != 0) {
            os << ", ";
        }
        os << toPrint.bucket[i];
    }
    os << "], i:chain[i]:referenceTable[i]=[";
    for (uint64_t i = 0; i < toPrint.tuplesInBucket; ++i) {
        os << "\n\t"
           << i
           << ":\t"
           << toPrint.chain[i]
           << ":\t"
           << *(toPrint.referenceTable[i]);
    }
    os << "]]";
    return os;
}

