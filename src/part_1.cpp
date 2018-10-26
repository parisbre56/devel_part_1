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
#define RELS 4000000

const uint32_t buckets = 1 << HASH_BITS; //2^n
const uint32_t hashMask = (1 << HASH_BITS) - 1;

int main(int argc, char* argv[]) {
    ConsoleOutput::debugEnabledDefault = false;
    ConsoleOutput consoleOutput("Main");

    consoleOutput.errorOutput("Hash bits are: " + to_string(HASH_BITS));
    consoleOutput.errorOutput("Hash mask is: " + to_string(hashMask));
    consoleOutput.errorOutput("buckets are: " + to_string(buckets));
    consoleOutput.errorOutput("subBuckets are: " + to_string(SUB_BUCKETS));
    consoleOutput.errorOutput("RELR are: " + to_string(RELR));
    consoleOutput.errorOutput("RELS are: " + to_string(RELS));
    consoleOutput.errorOutput("DIFF is: " + to_string(DIFF));
    consoleOutput.errorOutput("Result block size tuples are: "
                               + to_string(RESULT_H_BLOCK_SIZE));
    consoleOutput.errorOutput("Result block size bytes are: "
                               + to_string(RESULT_H_BLOCK_SIZE
                                           * sizeof(Tuple)));

    consoleOutput.errorOutput("PART_1 EXECUTION STARTED");
    clock_t start = clock();

    CO_IFDEBUG(consoleOutput, "Generating test data");

    //Generate R
    CO_IFDEBUG(consoleOutput, "Generating table R");
    Relation relR(RELR);
    for (uint32_t i = 1; i <= RELR; ++i) {
        Tuple temp(i, i);
        relR.addTuple(temp);
    }
    CO_IFDEBUG(consoleOutput, "Table R generated");
    CO_IFDEBUG(consoleOutput, relR.toString());

    //Generate S
    CO_IFDEBUG(consoleOutput, "Generating table S");
    Relation relS(RELS);
    for (uint32_t i = 1; i <= RELS; i += DIFF) {
        for (uint32_t j = i; j < i + DIFF; ++j) {
            Tuple temp(j, i);
            relS.addTuple(temp);
        }
    }
    CO_IFDEBUG(consoleOutput, "Table S generated");
    CO_IFDEBUG(consoleOutput, relS.toString());

    //Perform join
    CO_IFDEBUG(consoleOutput, "Starting radixHashJoin");
    clock_t joinStart = clock();
    Result result(radixHashJoin(relR, relS));
    clock_t end = clock();
    CO_IFDEBUG(consoleOutput, "radixHashJoin finished");
    CO_IFDEBUG(consoleOutput, result.toString());

    consoleOutput.errorOutput("PART_1 EXECUTION ENDED");
    consoleOutput.errorOutput("Load Time: "
                               + to_string((joinStart - start)
                                           / (double) CLOCKS_PER_SEC));
    consoleOutput.errorOutput("Join Time: "
                               + to_string((end - joinStart)
                                           / (double) CLOCKS_PER_SEC));
    consoleOutput.errorOutput("Total Time: "
                               + to_string((end - start)
                                           / (double) CLOCKS_PER_SEC));

    //cout << result.toString() << endl;

    return 0;
}

Result radixHashJoin(Relation& relR, Relation& relS) {
    ConsoleOutput consoleOutput("RadixHashJoin");
    consoleOutput.errorOutput("JOIN EXECUTION STARTED");

    HashTable rHash(relR, buckets, hashFunc);
    HashTable sHash(relS, buckets, hashFunc);

    CO_IFDEBUG(consoleOutput, "rHash=" + rHash.toString());
    CO_IFDEBUG(consoleOutput, "sHash=" + sHash.toString());

    Result retResult;
    for (uint32_t i = 0; i < buckets; ++i) {
        CO_IFDEBUG(consoleOutput, "Processing bucket " + to_string(i));

        if (rHash.getTuplesInBucket(i) == 0
            || sHash.getTuplesInBucket(i) == 0) {
            CO_IFDEBUG(consoleOutput,
                       "Skipping bucket "
                       + to_string(i)
                       + ": 0 rows [R="
                       + to_string(rHash.getTuplesInBucket(i))
                       + ", S="
                       + to_string(sHash.getTuplesInBucket(i))
                       + "]");
            continue;
        }

        BucketAndChain rChain(rHash, i,
        SUB_BUCKETS,
                              hashFuncChain);
        CO_IFDEBUG(consoleOutput, "Created subHashTable " + rChain.toString());
        rChain.join(sHash, i, retResult);
    }

    consoleOutput.errorOutput("JOIN EXECUTION ENDED");
    return retResult;
}

uint32_t hashFunc(uint32_t buckets, int32_t toHash) {
    //We ignore buckets, we don't really need it
    return hashMask & toHash;
}

uint32_t hashFuncChain(uint32_t buckets, int32_t toHash) {
    return toHash % buckets;
}
