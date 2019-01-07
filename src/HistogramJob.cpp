/*
 * HistogramJob.cpp
 *
 *  Created on: 6 Ιαν 2019
 *      Author: parisbre56
 */

#include "HistogramJob.h"

using namespace std;

HistogramJob::HistogramJob(const Relation& relation,
                           const HashFunction& hashFunction,
                           uint64_t segmentStartInclusive,
                           uint64_t segmentSize) :
        Callable(),
        relation(relation),
        hashFunction(hashFunction),
        segmentStartInclusive(segmentStartInclusive),
        segmentSize(segmentSize),
        result(new uint64_t[hashFunction.getBuckets()] {/* init to 0 */}) {

}

HistogramJob::~HistogramJob() {
    if (result != nullptr) {
        delete[] result;
    }
}

void HistogramJob::printSelf(ostream& os) const {
    os << "[HistogramJob finished="
       << finished
       << ", relation="
       << relation
       << ", hashFunction="
       << hashFunction
       << ", segmentStartInclusive="
       << segmentStartInclusive
       << ", segmentSize="
       << segmentSize
       << ", result=";
    if (result == nullptr) {
        os << "null";
    }
    else {
        os << "[";
        for (uint32_t i = 0; i < hashFunction.getBuckets(); ++i) {
            if (i != 0) {
                os << ", ";
            }
            os << result[i];
        }
        os << "]";
    }
    os << "]";
}

uint64_t* HistogramJob::getResultInternal() const {
    return result;
}

void HistogramJob::runInternal() {
    uint64_t segmentEndExclusive = segmentStartInclusive + segmentSize;
    for (uint64_t i = segmentStartInclusive; i < segmentEndExclusive; ++i) {
        const Tuple& toCheck = relation.getTuple(i);
        uint32_t currHash = hashFunction.applyHash(toCheck.getPayload(0));
        result[currHash]++;
    }
}

const HashFunction& HistogramJob::getHashFunction() const {
    return hashFunction;
}

const Relation& HistogramJob::getRelation() const {
    return relation;
}

const uint64_t HistogramJob::getSegmentSize() const {
    return segmentSize;
}

const uint64_t HistogramJob::getSegmentStartInclusive() const {
    return segmentStartInclusive;
}

ostream& operator<<(ostream& os, const HistogramJob& toPrint) {
    toPrint.printSelf(os);
    return os;
}
