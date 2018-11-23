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
#include "ResultContainer.h"
#include "TableLoader.h"
#include "Metadata.h"

using namespace std;

ResultContainer radixHashJoin(Relation& relR, Relation& relS);
uint32_t hashFunc(uint32_t buckets, int32_t toHash);
uint32_t hashFuncChain(uint32_t buckets, int32_t toHash);
void removeTrailingNewlines(string& toProcess);
void processInputFile(ConsoleOutput& consoleOutput,
                      string& input,
                      TableLoader& tableLoader);
void processInputJoin(ConsoleOutput& consoleOutput,
                      string& input,
                      Metadata& metadata);

#define HASH_BITS 3
#define SUB_BUCKETS 3
#define DIFF 10
#define RELR 3
#define RELS 1

const uint32_t buckets = 1 << HASH_BITS; //2^n
const uint32_t hashMask = (1 << HASH_BITS) - 1;

//TODO result size is related to input size

int main(int argc, char* argv[]) {
    ConsoleOutput::debugEnabledDefault = true;
    ConsoleOutput consoleOutput("Main");

    try {
        consoleOutput.errorOutput() << "Hash bits are: " << HASH_BITS << endl;
        consoleOutput.errorOutput() << "Hash mask is: " << hashMask << endl;
        consoleOutput.errorOutput() << "buckets are: " << buckets << endl;
        consoleOutput.errorOutput() << "subBuckets are: "
                                    << SUB_BUCKETS
                                    << endl;
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

        TableLoader tableLoader(100);

        //Throw exception on failure
        cin.exceptions(iostream::failbit | iostream::badbit);

        //For ease of testing, get input from arguments
        int currArg = 1;
        if (currArg < argc) {
            string input;
            for (; currArg < argc; ++currArg) {
                input = argv[currArg];

                if (processInputFile(consoleOutput, input, tableLoader))
                    break;
            }

            CO_IFDEBUG(consoleOutput,
                       "Loaded tables from args: " << tableLoader);
        }
        else {
            string input;
            do {
                cout << "Give the path to an input file or write 'Done' to stop giving input files: "
                     << endl;
                getline(cin, input);
                if (cin.fail() || cin.eof()) {
                    consoleOutput.errorOutput() << "cin terminated unexpectedly. Exiting..."
                                                << endl;
                }

            } while (!processInputFile(consoleOutput, input, tableLoader));

            CO_IFDEBUG(consoleOutput, "Loaded tables from cin: " << tableLoader);
        }

        clock_t joinStart = clock();

        //For ease of testing, get input from arguments
        Metadata metadata(tableLoader);
        if (currArg < argc) {
            string input;
            for (; currArg < argc; ++currArg) {
                input = argv[currArg];

                if (processInputJoin(consoleOutput, input, metadata))
                    break;
            }
            if (currArg == argc) {
                input = "Done";
                processInputJoin(consoleOutput, input, metadata);
            }

            CO_IFDEBUG(consoleOutput, "Finished processing args");
        }
        else {
            string input;
            do {
                cout << "Give a join expression, F to end the batch or Done to exit: " <<endl;
                if (cin.eof()) {
                    input = "Done";
                }
                else {
                    getline(cin, input);
                }
                if (cin.fail()) {
                    consoleOutput.errorOutput() << "cin terminated unexpectedly. Exiting..."
                                                << endl;
                }
            } while (!processInputJoin(consoleOutput, input, metadata));

            CO_IFDEBUG(consoleOutput, "Finished processing cin");
        }

        clock_t end = clock();

        consoleOutput.errorOutput() << "PART_1 EXECUTION ENDED" << endl;
        consoleOutput.errorOutput() << "Load Time: "
                                    << ((joinStart - start)
                                        / (double) CLOCKS_PER_SEC)
                                    << endl;
        consoleOutput.errorOutput() << "Join Time: "
                                    << ((end - joinStart)
                                        / (double) CLOCKS_PER_SEC)
                                    << endl;
        consoleOutput.errorOutput() << "Total Time: "
                                    << ((end - start) / (double) CLOCKS_PER_SEC)
                                    << endl;

        //cout << result << endl;

        return 0;
    }
    catch (const exception& ex) {
        cerr << "Error occurred: " << ex.what() << endl;
    }
    catch (...) {
        cerr << "Unknown failure occurred" << endl;
    }
}

ResultContainer radixHashJoin(Relation& relR, Relation& relS) {
    ConsoleOutput consoleOutput("RadixHashJoin");
    consoleOutput.errorOutput() << "JOIN EXECUTION STARTED" << endl;

    HashTable rHash(relR, buckets, hashFunc);
    HashTable sHash(relS, buckets, hashFunc);
    CO_IFDEBUG(consoleOutput, "Hashes generated");
    CO_IFDEBUG(consoleOutput, "rHash=" << rHash);
    CO_IFDEBUG(consoleOutput, "sHash=" << sHash);

    ResultContainer retResult;
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

void removeTrailingNewlines(string& toProcess) {
    while (toProcess.back() == '\n') {
        toProcess.pop_back();
    }
}

bool processInputFile(ConsoleOutput& consoleOutput,
                      string& input,
                      TableLoader& tableLoader) {
    CO_IFDEBUG(consoleOutput, "Got input: " << input);
    removeTrailingNewlines(input);
    if (input == "Done") {
        CO_IFDEBUG(consoleOutput, "Got Done. Finished loading files.");
        return true;
    }
    CO_IFDEBUG(consoleOutput, "Loading file '" << input << "'");
    tableLoader.loadTable(input);
    return false;
}
