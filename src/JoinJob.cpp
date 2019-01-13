/*
 * JoinJob.cpp
 *
 *  Created on: 7 Ιαν 2019
 *      Author: pibre
 */

#include "JoinJob.h"

#include "HashFunctionModulo.h"
#include "BucketAndChain.h"
#include "ConsoleOutput.h"

using namespace std;

uint32_t getBucketAndChainBuckets(const uint64_t tuplesInBucket);

JoinJob::JoinJob(HashTableJob& rHash,
                 HashTableJob& sHash,
                 const uint32_t bucket,
                 const bool* usedRows) :
        Callable(),
        rHash(rHash),
        sHash(sHash),
        bucket(bucket),
        usedRows(usedRows),
        result(nullptr) {

}

JoinJob::JoinJob(HashTableJob& rHash,
                 HashTableJob& sHash,
                 const uint32_t bucket,
                 const bool* usedRows,
                 ResultContainer* toReuse) :
        Callable(),
        rHash(rHash),
        sHash(sHash),
        bucket(bucket),
        usedRows(usedRows),
        result(toReuse) {

}

JoinJob::JoinJob(HashTableJob& rHash,
                 HashTableJob& sHash,
                 const uint32_t bucket,
                 const bool* usedRows,
                 Result* toReuse) :
        Callable(),
        rHash(rHash),
        sHash(sHash),
        bucket(bucket),
        usedRows(usedRows),
        result(new ResultContainer(toReuse->getRelation().getArraySize(),
                                   toReuse->getRelation().getSizeTableRows(),
                                   0,
                                   usedRows,
                                   toReuse)) {

}

JoinJob::~JoinJob() {
    if (result != nullptr) {
        delete result;
    }
}

void JoinJob::printSelf(std::ostream& os) const {
    os << "[JoinJob finished="
       << finished
       << ", rHash="
       << rHash
       << ", sHash="
       << sHash
       << ", bucket="
       << bucket
       << ", result=";
    if (result == nullptr) {
        os << "null";
    }
    else {
        os << *result;
    }
    os << "]";
}
ResultContainer* JoinJob::getResultInternal() {
    return result;
}
void JoinJob::runInternal() {
    ConsoleOutput consoleOutput("JoinJob::getResultInternal");
    CO_IFDEBUG(consoleOutput, "Processing bucket " << bucket);

    const HashTable* const rHashRes = rHash.waitAndGetResult();
    const HashTable* sHashRes =
            sHash.getFinished() ? sHash.waitAndGetResult() : nullptr;
    if (result == nullptr) {
        result = new ResultContainer((sHashRes == nullptr) ? ((rHashRes->getTuplesInBucket(bucket))
                                                              * (rHashRes->getTuplesInBucket(bucket))) :
                                                             ((rHashRes->getTuplesInBucket(bucket))
                                                              * (sHashRes->getTuplesInBucket(bucket))),
                                     rHashRes->getSizeTableRows(),
                                     0,
                                     usedRows);
    }
    if (rHashRes->getTuplesInBucket(bucket) == 0) {
        CO_IFDEBUG(consoleOutput,
                   "Skipping bucket " << bucket << ": 0 rows [R=" << rHashRes->getTuplesInBucket(bucket) << "]");
        return;
    }
    //If we have the s table, check if it is empty now
    if (sHashRes != nullptr && sHashRes->getTuplesInBucket(bucket) == 0) {
        CO_IFDEBUG(consoleOutput,
                   "Skipping bucket " << bucket << ": 0 rows [S=" << sHashRes->getTuplesInBucket(bucket) << "]");
        return;
    }
    //While we wait for s to finish, build the bucket and chain for r
    HashFunctionModulo modulo(getBucketAndChainBuckets(rHashRes->getTuplesInBucket(bucket)));
    BucketAndChain rChain(*rHashRes, bucket, modulo);
    CO_IFDEBUG(consoleOutput, "Created subHashTable " << rChain);
    //If we didn't check before, check S now
    if (sHashRes == nullptr) {
        sHashRes = sHash.waitAndGetResult();
        if (sHashRes->getTuplesInBucket(bucket) == 0) {
            CO_IFDEBUG(consoleOutput,
                       "Skipping bucket " << bucket << ": 0 rows [S=" << sHashRes->getTuplesInBucket(bucket) << "]");
            return;
        }
    }
    //Finally, do the join
    rChain.join(*sHashRes, bucket, *result);
}

ostream& operator<<(ostream& os, const JoinJob& toPrint) {
    toPrint.printSelf(os);
    return os;
}

uint32_t getBucketAndChainBuckets(const uint64_t tuplesInBucket) {
    if (tuplesInBucket < 100) {
        return 10;
    }
    if (tuplesInBucket < 1000) {
        return 20;
    }
    if (tuplesInBucket < 10000) {
        return 30;
    }
    if (tuplesInBucket < 100000) {
        return 40;
    }
    if (tuplesInBucket < 1000000) {
        return 50;
    }
    if (tuplesInBucket < 3000000) {
        return 60;
    }
    if (tuplesInBucket < 5000000) {
        return 70;
    }
    if (tuplesInBucket < 7000000) {
        return 80;
    }
    if (tuplesInBucket < 10000000) {
        return 90;
    }
    if (tuplesInBucket < 40000000) {
        return 100;
    }
    if (tuplesInBucket < 70000000) {
        return 110;
    }
    if (tuplesInBucket < 100000000) {
        return 120;
    }
    return 130;
}
