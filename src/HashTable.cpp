/*
 * HashTable.cpp
 *
 *  Created on: 22 ��� 2018
 *      Author: pibre
 */

#include "HashTable.h"

#include <stdexcept>

using namespace std;

HashTable::HashTable(const Relation& relation,
                     uint32_t buckets,
                     uint32_t (* const hashFunction)(uint32_t, int32_t),
                     const ConsoleOutput * const consoleOutput) :
        buckets(buckets),
        numTuples(relation.getNumTuples()),
        hashFunction(hashFunction),
        consoleOutput(consoleOutput),
        histogram(new uint32_t[buckets] { }),
        pSum(new uint32_t[buckets] { }),
        orderedTuples(new const Tuple*[relation.getNumTuples()] { }) {
    if (this->buckets <= 0) {
        throw runtime_error("buckets must be positive [buckets="
                            + to_string(this->buckets)
                            + "]");
    }
    if (numTuples <= 0) {
        throw runtime_error("numTuples must be positive [numTuples="
                            + to_string(numTuples)
                            + "]");
    }

    debug("Splitting "
          + to_string(numTuples)
          + " tuples to "
          + to_string(this->buckets)
          + " buckets");

    debug("Generating histogram");
    for (uint32_t i = 0; i < numTuples; ++i) {
        debug("Processing tuple " + to_string(i));
        const Tuple& toCheck = relation.getTuple(i);
        debug(to_string(i) + ":" + toCheck.toString());

        uint32_t currHash = this->hashFunction(buckets, toCheck.getPayload());
        debug("Assigned to bucket " + to_string(currHash));
        histogram[currHash]++;
    }
    if (consoleOutput != nullptr && consoleOutput->getDebugEnabled()) {
        consoleOutput->debugOutput("Created histogram with "
                                   + to_string(buckets)
                                   + " buckets");

        for (uint32_t i = 0; i < buckets; ++i) {
            consoleOutput->debugOutput("[bucket="
                                       + to_string(i)
                                       + ", size="
                                       + to_string(histogram[i])
                                       + "]");
        }
    }

    debug("Generating psum");
    {
        uint32_t prevSum = 0;
        for (uint32_t i = 0; i < buckets; ++i) {
            debug("[bucket="
                  + to_string(i)
                  + ", pSum="
                  + to_string(prevSum)
                  + "]");
            pSum[i] = prevSum;
            prevSum += histogram[i];
            histogram[i] = 0; //Just so that we don't create a new array, we reuse the histogram array
        }
    }
    debug("Psum generated");

    debug("Copying Tuples");
    for (uint32_t i = 0; i < numTuples; ++i) {
        debug("Processing tuple " + to_string(i));
        const Tuple& toCheck = relation.getTuple(i);
        debug(to_string(i) + ":" + toCheck.toString());

        uint32_t currHash = this->hashFunction(buckets, toCheck.getPayload());
        debug("Copying to bucket "
              + to_string(currHash)
              + " position "
              + to_string(histogram[currHash]));
        orderedTuples[pSum[currHash] + histogram[currHash]] = new Tuple(toCheck);
        histogram[currHash]++;
    }
    debug("Tuples copied");
}

/*HashTable::HashTable(const HashTable & toCopy) {
    //TODO copy constructor
 }*/

HashTable::~HashTable() {
    delete histogram;
    delete pSum;
    for (uint32_t i = 0; i < numTuples; ++i) {
        const Tuple* currTuple = orderedTuples[i];
        if (currTuple != nullptr) {
            delete currTuple;
        }
    }
    delete orderedTuples;
}

void HashTable::debug(string outString) {
    if (consoleOutput != nullptr) {
        consoleOutput->debugOutput(outString);
    }
}

void HashTable::error(string outString) {
    if (consoleOutput != nullptr) {
        consoleOutput->errorOutput(outString);
    }
}
