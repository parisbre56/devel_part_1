// Name        : part_1.cpp
// Author      : sdi1100070
// Version     :
// Copyright   : No copyright
// Description : Part 1 of Devel project
//============================================================================

#include <iostream>
#include <string>

#include <cstdint>
#include <cmath>
#include <ctime>

#include "ConsoleOutput.h"
#include "BucketAndChain.h"
#include "Tuple.h"
#include "HashTable.h"
#include "Relation.h"
#include "Result.h"

using namespace std;

Result radixHashJoin(Relation& relR, Relation& relS);
uint32_t hashFunc(uint32_t buckets, int32_t toHash);
uint32_t hashFuncChain(uint32_t buckets, int32_t toHash);

#define HASH_BITS 24
#define SUB_BUCKETS 2048
#define DIFF 10
#define RELR 4000000
#define RELS 4000

const uint32_t buckets = 1 << HASH_BITS; //2^n
const uint32_t hashMask = (1 << HASH_BITS) - 1;

int main(int argc, char* argv[]) {
    ConsoleOutput::debugEnabledDefault = true;
    ConsoleOutput consoleOutput("Main");

    consoleOutput.errorOutput() << "Hash bits are: " << HASH_BITS << endl;
    consoleOutput.errorOutput() << "Hash mask is: " << hashMask << endl;
    consoleOutput.errorOutput() << "buckets are: " << buckets << endl;
    consoleOutput.errorOutput() << "subBuckets are: " << SUB_BUCKETS << endl;
    consoleOutput.errorOutput() << "RELR are: " << RELR << endl;
    consoleOutput.errorOutput() << "RELS are: " << RELS << endl;
    consoleOutput.errorOutput() << "DIFF is: " << DIFF << endl;
    consoleOutput.errorOutput() << "Result block size tuples are: "
                                << RESULT_H_BLOCK_SIZE
                                << endl;
    consoleOutput.errorOutput() << "Result block size bytes are: "
                                << (RESULT_H_BLOCK_SIZE * sizeof(Tuple))
                                << endl;

    consoleOutput.errorOutput() << "PART_1 EXECUTION STARTED" << endl;
    clock_t start = clock();

    CO_IFDEBUG(consoleOutput, "Generating test data");

    //Generate R
    CO_IFDEBUG(consoleOutput, "Generating table R");
    Relation relR(RELR);
    for (uint32_t i = 1; i <= RELR; ++i) {
        Tuple temp(i, i % RELS);
        relR.addTuple(temp);
    }
    CO_IFDEBUG(consoleOutput, "Table R generated");
    CO_IFDEBUG(consoleOutput, relR);

    //Generate S
    CO_IFDEBUG(consoleOutput, "Generating table S");
    Relation relS(RELS);
    for (uint32_t i = 1; i <= RELS; ++i) {
        Tuple temp(i, i);
        relS.addTuple(temp);
    }
    CO_IFDEBUG(consoleOutput, "Table S generated");
    CO_IFDEBUG(consoleOutput, relS);

    //Perform join
    CO_IFDEBUG(consoleOutput, "Starting radixHashJoin");
    clock_t joinStart = clock();
    Result result(radixHashJoin(relR, relS));
    clock_t end = clock();
    CO_IFDEBUG(consoleOutput, "radixHashJoin finished");
    CO_IFDEBUG(consoleOutput, result);

    consoleOutput.errorOutput() << "PART_1 EXECUTION ENDED" << endl;
    consoleOutput.errorOutput() << "Load Time: "
                                << ((joinStart - start)
                                    / (double) CLOCKS_PER_SEC)
                                << endl;
    consoleOutput.errorOutput() << "Join Time: "
                                << ((end - joinStart) / (double) CLOCKS_PER_SEC)
                                << endl;
    consoleOutput.errorOutput() << "Total Time: "
                                << ((end - start) / (double) CLOCKS_PER_SEC)
                                << endl;

    //cout << result << endl;

    return 0;
}

Result radixHashJoin(Relation& relR, Relation& relS) {
    ConsoleOutput consoleOutput("RadixHashJoin");
    consoleOutput.errorOutput() << "JOIN EXECUTION STARTED" << endl;

    HashTable rHash(relR, buckets, hashFunc);
    HashTable sHash(relS, buckets, hashFunc);

    CO_IFDEBUG(consoleOutput, "rHash=" << rHash);
    CO_IFDEBUG(consoleOutput, "sHash=" << sHash);

    Result retResult;
    for (uint32_t i = 0; i < buckets; ++i) {
        CO_IFDEBUG(consoleOutput, "Processing bucket " << i);

        if (rHash.getTuplesInBucket(i) == 0
            || sHash.getTuplesInBucket(i) == 0) {
            CO_IFDEBUG(consoleOutput,
                       "Skipping bucket " << i << ": 0 rows [R=" << rHash.getTuplesInBucket(i) << ", S=" << sHash.getTuplesInBucket(i) << "]");
            continue;
        }

        BucketAndChain rChain(rHash, i,
        SUB_BUCKETS,
                              hashFuncChain);
        CO_IFDEBUG(consoleOutput, "Created subHashTable " << rChain);
        rChain.join(sHash, i, retResult);
    }

    consoleOutput.errorOutput() << "JOIN EXECUTION ENDED" << endl;
    return retResult;
}

uint32_t hashFunc(uint32_t buckets, int32_t toHash) {
    //We ignore buckets, we don't really need it
    return hashMask & toHash;
}

uint32_t hashFuncChain(uint32_t buckets, int32_t toHash) {
    return toHash % buckets;
}
