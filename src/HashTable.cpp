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
        orderedTuples(new Tuple[relation.getNumTuples()] { }) {
    ConsoleOutput consoleOutput("HashTable");
    if (this->buckets == 0) {
        throw runtime_error("buckets must be positive [buckets="
                            + to_string(this->buckets)
                            + "]");
    }
    if (numTuples == 0) {
        throw runtime_error("numTuples must be positive [numTuples="
                            + to_string(numTuples)
                            + "]");
    }

    CO_IFDEBUG(consoleOutput,
               "Splitting " << numTuples << " tuples to " << (this->buckets) << " buckets");

    CO_IFDEBUG(consoleOutput, "Generating histogram");
    for (uint32_t i = 0; i < numTuples; ++i) {
        CO_IFDEBUG(consoleOutput, "Processing tuple " << i);
        const Tuple& toCheck = relation.getTuple(i);
        CO_IFDEBUG(consoleOutput, i << ":" << toCheck);

        uint32_t currHash = this->hashFunction(buckets, toCheck.getPayload());
        CO_IFDEBUG(consoleOutput, "Assigned to bucket " << currHash);
        histogram[currHash]++;
    }
    if (consoleOutput.getDebugEnabled()) {
        CO_IFDEBUG(consoleOutput,
                   "Created histogram with " << buckets << " buckets");

        for (uint32_t i = 0; i < buckets; ++i) {
            CO_IFDEBUG(consoleOutput,
                       "\t[bucket=" << i << ", size=" << histogram[i] << "]");
        }
    }

    CO_IFDEBUG(consoleOutput, "Generating pSum");
    {
        uint32_t prevSum = 0;
        for (uint32_t i = 0; i < buckets; ++i) {
            CO_IFDEBUG(consoleOutput,
                       "\t[bucket=" << i << ", pSum=" << prevSum << "]");
            pSum[i] = prevSum;
            prevSum += histogram[i];
            histogram[i] = 0; //Just so that we don't create a new array, we reuse the histogram array
        }
    }
    CO_IFDEBUG(consoleOutput, "pSum generated");

    CO_IFDEBUG(consoleOutput, "Copying Tuples");
    for (uint32_t i = 0; i < numTuples; ++i) {
        CO_IFDEBUG(consoleOutput, "Processing tuple " << i);
        const Tuple& toCheck = relation.getTuple(i);
        CO_IFDEBUG(consoleOutput, i << ":" << toCheck);

        uint32_t currHash = this->hashFunction(buckets, toCheck.getPayload());
        CO_IFDEBUG(consoleOutput,
                   "Copying to bucket " << currHash << " position " << histogram[currHash]);
        orderedTuples[pSum[currHash] + histogram[currHash]] = toCheck;
        histogram[currHash]++;
    }
    CO_IFDEBUG(consoleOutput, "Tuples copied");
}

HashTable::HashTable(const HashTable & toCopy) :
        buckets(toCopy.buckets),
        numTuples(toCopy.numTuples),
        hashFunction(toCopy.hashFunction),
        histogram(new uint32_t[buckets]),
        pSum(new uint32_t[buckets]),
        orderedTuples(new Tuple[toCopy.numTuples]) {
    memcpy(histogram, toCopy.histogram, buckets * sizeof(uint32_t));
    memcpy(pSum, toCopy.pSum, buckets * sizeof(uint32_t));
    memcpy(orderedTuples, toCopy.orderedTuples, numTuples * sizeof(Tuple));
}

HashTable::~HashTable() {
    delete[] histogram;
    delete[] pSum;
    delete[] orderedTuples;
}

uint32_t HashTable::getBuckets() const {
    return buckets;
}

uint32_t HashTable::getTuplesInBucket(uint32_t bucket) const {
    if (bucket >= buckets) {
        throw runtime_error("bucket out of bounds [bucket="
                            + to_string(bucket)
                            + ", buckets="
                            + to_string(buckets)
                            + "]");
    }

    return histogram[bucket];
}

const Tuple * const HashTable::getBucket(uint32_t bucket) const {
    if (bucket >= buckets) {
        throw runtime_error("bucket out of bounds [bucket="
                            + to_string(bucket)
                            + ", buckets="
                            + to_string(buckets)
                            + "]");
    }

    return orderedTuples + pSum[bucket];
}

const Tuple& HashTable::getTuple(uint32_t bucket, uint32_t index) const {
    if (bucket >= buckets) {
        throw runtime_error("bucket out of bounds [bucket="
                            + to_string(bucket)
                            + ", buckets="
                            + to_string(buckets)
                            + "]");
    }
    if (index >= histogram[bucket]) {
        throw runtime_error("bucket index out of bounds [bucket="
                            + to_string(bucket)
                            + ", index="
                            + to_string(index)
                            + ", histogram="
                            + to_string(histogram[bucket])
                            + "]");
    }

    return orderedTuples[pSum[bucket] + index];
}

std::ostream& operator<<(std::ostream& os, const HashTable& toPrint) {
    os << "[HashTable buckets="
       << toPrint.buckets
       << ", numTuples="
       << toPrint.numTuples
       << ", hashFunction="
       << ((void*) toPrint.hashFunction)
       << ", histogram=[";
    for (uint32_t i = 0; i < toPrint.buckets; ++i) {
        if (i != 0) {
            os << ", ";
        }
        os << toPrint.histogram[i];
    }
    os << "], pSum=[";
    for (uint32_t i = 0; i < toPrint.buckets; ++i) {
        if (i != 0) {
            os << ", ";
        }
        os << toPrint.pSum[i];
    }
    os << "], orderedTuples=[";
    for (uint32_t i = 0; i < toPrint.buckets; ++i) {
        os << "\n\tBucket #" << i;
        uint32_t bucketStart = toPrint.pSum[i];
        uint32_t numInBucket = toPrint.histogram[i];
        for (uint32_t j = 0; j < numInBucket; ++j) {
            uint32_t pos = bucketStart + j;
            os << "\n\t\t<"
               << pos
               << ","
               << j
               << ">: "
               << toPrint.orderedTuples[pos];
        }
    }
    os << "]]";
    return os;
}
