/*
 * HashTable.cpp
 *
 *  Created on: 22 ��� 2018
 *      Author: pibre
 */

#include "HashTable.h"

#include <stdexcept>
#include <sstream>

#include <cstring>

using namespace std;

HashTable::HashTable(const Relation& relation,
                     uint32_t buckets,
                     uint32_t (* const hashFunction)(uint32_t, int32_t)) :
        buckets(buckets),
        numTuples(relation.getNumTuples()),
        hashFunction(hashFunction),
        histogram(new uint32_t[buckets] { }),
        pSum(new uint32_t[buckets] { }),
        orderedTuples(new const Tuple*[relation.getNumTuples()] { }) {
    ConsoleOutput consoleOutput("HashTable");
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

    CO_IFDEBUG(consoleOutput,
               "Splitting "
          + to_string(numTuples)
          + " tuples to "
          + to_string(this->buckets)
          + " buckets");

    CO_IFDEBUG(consoleOutput, "Generating histogram");
    for (uint32_t i = 0; i < numTuples; ++i) {
        CO_IFDEBUG(consoleOutput, "Processing tuple " + to_string(i));
        const Tuple& toCheck = relation.getTuple(i);
        CO_IFDEBUG(consoleOutput, to_string(i) + ":" + toCheck.toString());

        uint32_t currHash = this->hashFunction(buckets, toCheck.getPayload());
        CO_IFDEBUG(consoleOutput, "Assigned to bucket " + to_string(currHash));
        histogram[currHash]++;
    }
    if (consoleOutput.getDebugEnabled()) {
        consoleOutput.debugOutput("Created histogram with "
                                   + to_string(buckets)
                                   + " buckets");

        for (uint32_t i = 0; i < buckets; ++i) {
            consoleOutput.debugOutput("[bucket="
                                       + to_string(i)
                                       + ", size="
                                       + to_string(histogram[i])
                                       + "]");
        }
    }

    CO_IFDEBUG(consoleOutput, "Generating psum");
    {
        uint32_t prevSum = 0;
        for (uint32_t i = 0; i < buckets; ++i) {
            CO_IFDEBUG(consoleOutput,
                       "[bucket="
                  + to_string(i)
                  + ", pSum="
                  + to_string(prevSum)
                  + "]");
            pSum[i] = prevSum;
            prevSum += histogram[i];
            histogram[i] = 0; //Just so that we don't create a new array, we reuse the histogram array
        }
    }
    CO_IFDEBUG(consoleOutput, "Psum generated");

    CO_IFDEBUG(consoleOutput, "Copying Tuples");
    for (uint32_t i = 0; i < numTuples; ++i) {
        CO_IFDEBUG(consoleOutput, "Processing tuple " + to_string(i));
        const Tuple& toCheck = relation.getTuple(i);
        CO_IFDEBUG(consoleOutput, to_string(i) + ":" + toCheck.toString());

        uint32_t currHash = this->hashFunction(buckets, toCheck.getPayload());
        CO_IFDEBUG(consoleOutput,
                   "Copying to bucket "
              + to_string(currHash)
              + " position "
              + to_string(histogram[currHash]));
        orderedTuples[pSum[currHash] + histogram[currHash]] = new Tuple(toCheck);
        histogram[currHash]++;
    }
    CO_IFDEBUG(consoleOutput, "Tuples copied");
}

HashTable::HashTable(const HashTable & toCopy) :
        buckets(toCopy.buckets),
        numTuples(toCopy.numTuples),
        hashFunction(toCopy.hashFunction),
        histogram(new uint32_t[buckets] { }),
        pSum(new uint32_t[buckets] { }),
        orderedTuples(new const Tuple*[toCopy.numTuples] { }) {
    memcpy(histogram, toCopy.histogram, buckets * sizeof(uint32_t));
    memcpy(pSum, toCopy.pSum, buckets * sizeof(uint32_t));
    for (uint32_t i = 0; i < numTuples; ++i) {
        orderedTuples[i] = new Tuple(*(toCopy.orderedTuples[i]));
    }
}

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

uint32_t HashTable::getBuckets() const {
    return buckets;
}

uint32_t HashTable::getTuplesInBucket(uint32_t bucket) const {
    if (bucket < 0 || bucket >= buckets) {
        throw runtime_error("bucket out of bounds [bucket="
                            + to_string(bucket)
                            + ", buckets="
                            + to_string(buckets)
                            + "]");
    }

    return histogram[bucket];
}

const Tuple * const * const HashTable::getBucket(uint32_t bucket) const {
    if (bucket < 0 || bucket >= buckets) {
        throw runtime_error("bucket out of bounds [bucket="
                            + to_string(bucket)
                            + ", buckets="
                            + to_string(buckets)
                            + "]");
    }

    return orderedTuples + pSum[bucket];
}

const Tuple& HashTable::getTuple(uint32_t bucket, uint32_t index) const {
    if (bucket < 0 || bucket >= buckets) {
        throw runtime_error("bucket out of bounds [bucket="
                            + to_string(bucket)
                            + ", buckets="
                            + to_string(buckets)
                            + "]");
    }
    if (index < 0 || index >= histogram[bucket]) {
        throw runtime_error("bucket index out of bounds [bucket="
                            + to_string(bucket)
                            + ", index="
                            + to_string(index)
                            + ", histogram="
                            + to_string(histogram[bucket])
                            + "]");
    }

    return *(orderedTuples[pSum[bucket] + index]);
}

string HashTable::toString() const {
    ostringstream retVal;
    retVal << "[HashTable buckets="
           << buckets
           << ", numTuples="
           << numTuples
           << ", hashFunction="
           << (void*) hashFunction
           << ", histogram=[";
    for (uint32_t i = 0; i < buckets; ++i) {
        if (i != 0) {
            retVal << ", ";
        }
        retVal << histogram[i];
    }
    retVal << "], pSum=[";
    for (uint32_t i = 0; i < buckets; ++i) {
        if (i != 0) {
            retVal << ", ";
        }
        retVal << pSum[i];
    }
    retVal << "], orderedTuples=[";
    for (uint32_t i = 0; i < buckets; ++i) {
        retVal << "\n\tBucket #" << i;
        uint32_t bucketStart = pSum[i];
        uint32_t numInBucket = histogram[i];
        for (uint32_t j = 0; j < numInBucket; ++j) {
            uint32_t pos = bucketStart + j;
            retVal << "\n\t\t<"
                   << pos
                   << ","
                   << j
                   << ">: "
            << (orderedTuples[pos])->toString();
        }
    }
    retVal << "]]";
    return retVal.str();
}
