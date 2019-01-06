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

HashTable::HashTable(Executor& executor,
                     const Relation& relation,
                     HashFunction& hashFunction) :
        executor(executor),
        buckets(hashFunction.getBuckets()),
        numTuples(relation.getNumTuples()),
        hashFunction(hashFunction),
        histogram(new uint64_t[hashFunction.getBuckets()] { }),
        pSum(new uint64_t[hashFunction.getBuckets()] { }),
        sizeTableRows(relation.getSizeTableRows()),
        sizePayloads(relation.getSizePayloads()),
        usedRows(relation.getUsedRows()),
        orderedTuples(new const Tuple*[relation.getNumTuples()] { }),
        segments(0),
        histogramJobs(nullptr),
        partitionJobs(nullptr) {
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
    segments = numTuples / HASHTABLE_H_SEGMENT_SIZE;

    histogramJobs = new HistogramJob*[segments] {/* init to nullptr */};
    for (uint64_t i = 0, currSegmentStart = 0; i < segments;
            ++i, currSegmentStart += HASHTABLE_H_SEGMENT_SIZE) {
        histogramJobs[i] = new HistogramJob(relation,
                                            hashFunction,
                                            currSegmentStart,
                                            HASHTABLE_H_SEGMENT_SIZE);
        executor.addToQueue(histogramJobs[i]);
    }

    //While we wait for the executor to run the jobs, this thread will process the remainder
    //Special case: if numTuples is less than segment size, then no job will be created.
    for (uint64_t i = segments * HASHTABLE_H_SEGMENT_SIZE; i < numTuples; ++i) {
        CO_IFDEBUG(consoleOutput, "Processing tuple " << i);
        const Tuple& toCheck = relation.getTuple(i);
        CO_IFDEBUG(consoleOutput, i << ":" << toCheck);

        uint32_t currHash = hashFunction.applyHash(toCheck.getPayload(0));
        CO_IFDEBUG(consoleOutput, "Assigned to bucket " << currHash);
        histogram[currHash]++;
    }
    //Aggregate segment histograms and generate copy jobs
    for (uint64_t currJobIndex = 0; currJobIndex < segments; ++currJobIndex) {
        HistogramJob& currJob = *(histogramJobs[currJobIndex]);
        uint64_t* currResult = currJob.waitAndGetResult();
        CO_IFDEBUG(consoleOutput,
                   "Processing result for histogram [currJobIndex="<<currJobIndex<<", currJob="<<currJob<<"]");
        for (uint64_t currBucketIndex = 0; currBucketIndex < buckets;
                ++currBucketIndex) {
            histogram[currBucketIndex] += currResult[currBucketIndex];
        }
    }
    if (consoleOutput.getDebugEnabled()) {
        CO_IFDEBUG(consoleOutput,
                   "Created histogram with " << buckets << " buckets");

        for (uint32_t i = 0; i < buckets; ++i) {
            CO_IFDEBUG(consoleOutput,
                       "\t[bucket=" << i << ", size=" << histogram[i] << "]");
        }
    }
    //FIXME Need this after pSum, use new table instead of pSum
    //First compute pSum then memcpy that to a new array and increment that as you go
    partitionJobs = new PartitionJob*[segments];
    for (uint64_t currJobIndex = 0; currJobIndex < segments; ++currJobIndex) {
        HistogramJob& currJob = *(histogramJobs[currJobIndex]);
        uint64_t* currResult = currJob.waitAndGetResult();
        partitionJobs[currJobIndex] = new PartitionJob(currJob,
                                                       histogram,
                                                       pSum,
                                                       orderedTuples);
        CO_IFDEBUG(consoleOutput,
                   "Processing result for partition job creation [currJobIndex="<<currJobIndex<<", currJob="<<currJob<<"]");
        for (uint64_t currBucketIndex = 0; currBucketIndex < buckets;
                ++currBucketIndex) {
            pSum[currBucketIndex] += currResult[currBucketIndex];
        }
    }

    CO_IFDEBUG(consoleOutput, "Generating pSum");
    {
        uint64_t prevSum = 0;
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
    for (uint64_t i = 0; i < numTuples; ++i) {
        CO_IFDEBUG(consoleOutput, "Processing tuple " << i);
        const Tuple& toCheck = relation.getTuple(i);
        CO_IFDEBUG(consoleOutput, i << ":" << toCheck);

        uint32_t currHash = hashFunction.applyHash(toCheck.getPayload(0));
        CO_IFDEBUG(consoleOutput,
                   "Copying to bucket " << currHash << " position " << histogram[currHash]);
        orderedTuples[pSum[currHash] + histogram[currHash]] = &toCheck;
        histogram[currHash]++;
    }
    CO_IFDEBUG(consoleOutput, "Tuples copied");

    //Delete jobs to free up space
    for (uint64_t i = 0; i < segments; ++i) {
        delete histogramJobs[i];
    }
    delete[] histogramJobs;
    for (uint64_t i = 0; i < segments; ++i) {
        delete partitionJobs[i];
    }
    delete[] partitionJobs;
}

HashTable::~HashTable() {
    delete[] histogram;
    delete[] pSum;
    delete[] orderedTuples;
    if (histogramJobs != nullptr) {
        for (uint64_t i = 0; i < segments; ++i) {
            if (histogramJobs[i] != nullptr) {
                delete histogramJobs[i];
            }
        }
        delete[] histogramJobs;
    }
    if (partitionJobs != nullptr) {
        for (uint64_t i = 0; i < segments; ++i) {
            if (partitionJobs[i] != nullptr) {
                delete partitionJobs[i];
            }
        }
        delete[] partitionJobs;
    }
}

uint32_t HashTable::getBuckets() const {
    return buckets;
}

uint64_t HashTable::getTuplesInBucket(uint32_t bucket) const {
    if (bucket >= buckets) {
        throw runtime_error("bucket out of bounds [bucket="
                            + to_string(bucket)
                            + ", buckets="
                            + to_string(buckets)
                            + "]");
    }

    return histogram[bucket];
}

const Tuple * const * HashTable::getBucket(uint32_t bucket) const {
    if (bucket >= buckets) {
        throw runtime_error("bucket out of bounds [bucket="
                            + to_string(bucket)
                            + ", buckets="
                            + to_string(buckets)
                            + "]");
    }
    return orderedTuples + pSum[bucket];
}

const Tuple& HashTable::getTuple(uint32_t bucket, uint64_t index) const {
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

    return *(orderedTuples[pSum[bucket] + index]);
}

const bool* HashTable::getUsedRows() const {
    return usedRows;
}

size_t HashTable::getSizePayloads() const {
    return sizePayloads;
}

uint32_t HashTable::getSizeTableRows() const {
    return sizeTableRows;
}

std::ostream& operator<<(std::ostream& os, const HashTable& toPrint) {
    os << "[HashTable buckets="
       << toPrint.buckets
       << ", numTuples="
       << toPrint.numTuples
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
    os << "], histogram=[";
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
        uint64_t bucketStart = toPrint.pSum[i];
        uint64_t numInBucket = toPrint.histogram[i];
        for (uint64_t j = 0; j < numInBucket; ++j) {
            uint64_t pos = bucketStart + j;
            os << "\n\t\t<"
               << pos
               << ","
               << j
               << ">: "
               << *(toPrint.orderedTuples[pos]);
        }
    }
    os << "]]";
    return os;
}
