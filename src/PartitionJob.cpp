/*
 * PartitionJob.cpp
 *
 *  Created on: 7 Ιαν 2019
 *      Author: parisbre56
 */

#include "PartitionJob.h"

#include <cstring>

using namespace std;

PartitionJob::PartitionJob(const HistogramJob& histogramJob,
                           const uint64_t * const inSegmentPSum,
                           const Tuple* * const outputTuples) :
        Callable(),
        histogramJob(histogramJob),
        outputTuples(outputTuples),
        segmentPSum(new uint64_t[histogramJob.getHashFunction().getBuckets()]) {
    memcpy(segmentPSum,
           inSegmentPSum,
           histogramJob.getHashFunction().getBuckets() * sizeof(uint64_t));
}

PartitionJob::~PartitionJob() {
    delete[] segmentPSum;
}

void PartitionJob::printSelf(ostream& os) const {
    os << "[PartitionJob finished="
       << finished
       << ", histogramJob="
       << histogramJob
       << ", outputTuples="
       << outputTuples
       << ", segmentPSum=";
    if (segmentPSum == nullptr) {
        os << "null";
    }
    else {
        os << "[";
        for (uint32_t i = 0; i < histogramJob.getHashFunction().getBuckets();
                ++i) {
            if (i != 0) {
                os << ", ";
            }
            os << segmentPSum[i];
        }
        os << "]";
    }
    os << "]";
}

void * PartitionJob::getResultInternal() const {
    return nullptr;
}

void PartitionJob::runInternal() {
    uint64_t segmentEndExclusive = histogramJob.getSegmentStartInclusive()
                                   + histogramJob.getSegmentSize();
    const Tuple* const * tuples = histogramJob.getRelation().getTuples();
    const HashFunction& hashFunction = histogramJob.getHashFunction();
    for (uint64_t i = histogramJob.getSegmentStartInclusive();
            i < segmentEndExclusive; ++i) {
        const Tuple* const toCheck = tuples[i];
        const uint32_t currHash = hashFunction.applyHash(toCheck->getPayload(0));
        uint64_t posToAdd = segmentPSum[currHash];
        outputTuples[posToAdd] = toCheck;
        segmentPSum[currHash]++;
    }
}

ostream& operator<<(ostream& os, const PartitionJob& toPrint) {
    toPrint.printSelf(os);
    return os;
}
