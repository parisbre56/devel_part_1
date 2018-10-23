//============================================================================
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

#include "ConsoleOutput.h"
#include "BucketAndChain.h"
#include "Tuple.h"
#include "HashTable.h"
#include "Relation.h"
#include "Result.h"

using namespace std;

//TODO: Change to correct object type?
Result radixHashJoin(Relation& relR, Relation& relS);
uint32_t hashFunc(uint32_t buckets, int32_t toHash);
uint32_t hashFuncChain(uint32_t buckets, int32_t toHash);

#define HASH_BITS 3
#define SUB_BUCKETS 10

const uint32_t buckets = 1 << HASH_BITS; //2^n
const uint32_t hashMask = (1 << HASH_BITS) - 1;

ConsoleOutput* consoleOutput;

int main(int argc, char* argv[]) {
    //Use this to automatically handle deletion while keeping it global.
    ConsoleOutput conOutT(true);
    consoleOutput = &conOutT;
    consoleOutput->debugOutput("Starting Part 1 execution");

    consoleOutput->debugOutput("Hash bits are: " + to_string(HASH_BITS));
    consoleOutput->debugOutput("Hash mask is: " + to_string(hashMask));

    consoleOutput->debugOutput("Generating test data");

    //Generate R
    consoleOutput->debugOutput("Generating table R");
    Relation relR;
    for (uint32_t i = 1; i <= 4; ++i) {
        Tuple temp(i, i);
        relR.addTuple(temp);
    }
    consoleOutput->debugOutput("Table R generated");
    consoleOutput->debugOutput(relR.toString());

    //Generate S
    consoleOutput->debugOutput("Generating table S");
    Relation relS;
    for (uint32_t i = 1; i <= 4; ++i) {
        Tuple temp(i, (i % 2 == 0) ? (i - 1) : i);
        relS.addTuple(temp);
    }
    consoleOutput->debugOutput("Table S generated");
    consoleOutput->debugOutput(relS.toString());

    //Perform join
    consoleOutput->debugOutput("Starting radixHashJoin");
    Result result(radixHashJoin(relR, relS));
    consoleOutput->debugOutput("radixHashJoin finished");
    consoleOutput->debugOutput(result.toString());

    return 0;
}

Result radixHashJoin(Relation& relR, Relation& relS) {
    HashTable rHash(relR, buckets, hashFunc, consoleOutput);
    HashTable sHash(relS, buckets, hashFunc, consoleOutput);

    consoleOutput->debugOutput("rHash=" + rHash.toString());
    consoleOutput->debugOutput("sHash=" + sHash.toString());

    Result retResult;
    for (uint32_t i = 0; i < buckets; ++i) {
        if (rHash.getTuplesInBucket(i) == 0
            || sHash.getTuplesInBucket(i) == 0) {
            consoleOutput->debugOutput("Skipping bucket "
                                       + to_string(i)
                                       + ": 0 rows [R="
                                       + to_string(rHash.getTuplesInBucket(i))
                                       + ", S="
                                       + to_string(sHash.getTuplesInBucket(i))
                                       + "]");
            continue;
        }

        BucketAndChain rChain(rHash,
                              i,
                              SUB_BUCKETS,
                              hashFuncChain,
                              consoleOutput);
        rChain.join(sHash, i, retResult);
    }

    return retResult;
}

uint32_t hashFunc(uint32_t buckets, int32_t toHash) {
    //We ignore buckets, we don't really need it
    return hashMask & toHash;
}

uint32_t hashFuncChain(uint32_t buckets, int32_t toHash) {
    return toHash % buckets;
}
