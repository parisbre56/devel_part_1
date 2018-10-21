//============================================================================
// Name        : part_1.cpp
// Author      : sdi1100070
// Version     :
// Copyright   : No copyright
// Description : Hello World in C++, Ansi-style
//============================================================================

#include <iostream>
#include <string>
#include <cstdint>
#include <cmath>

#include "consoleOutput.h"
#include "Relation.h"
#include "Tuple.h"

using namespace std;

//TODO: Change to correct object type
Relation radixHashJoin(Relation& relR, Relation& relS);
uint32_t hashFunc(int32_t toHash);

#define HASH_BITS 3

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
    Relation result = radixHashJoin(relR, relS);
    consoleOutput->debugOutput("radixHashJoin finished");
    consoleOutput->debugOutput(result.toString());

    return 0;
}

Relation radixHashJoin(Relation& relR, Relation& relS) {
    Relation retRelation;
    uint32_t buckets = 1 << HASH_BITS; //2^n
    consoleOutput->debugOutput("Splitting R to "
                               + to_string(buckets)
                               + " buckets");
    uint32_t histogram[buckets] = { }; //Init all to 0
    for (uint32_t i = 0; i < relR.getNumTuples(); ++i) {

    }

    return retRelation;
}

uint32_t hashFunc(int32_t toHash) {
    return hashMask & toHash;
}
