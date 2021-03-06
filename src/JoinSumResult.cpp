/*
 * JoinSumResult.cpp
 *
 *  Created on: 27 Νοε 2018
 *      Author: parisbre56
 */

#include "JoinSumResult.h"

#include <stdexcept>

using namespace std;

JoinSumResult::JoinSumResult(uint32_t numOfSums) :
        numOfSums(numOfSums),
        sums(new uint64_t[numOfSums] { /*initialize to 0*/}),
        hasResults(false) {

}
JoinSumResult::JoinSumResult(JoinSumResult&& toMove) {
    numOfSums = toMove.numOfSums;
    sums = toMove.sums;
    hasResults = toMove.hasResults;
    toMove.sums = nullptr;
}
JoinSumResult& JoinSumResult::operator=(JoinSumResult&& toMove) {
    if (this == &toMove) {
        return *this;
    }
    if (sums != nullptr) {
        delete[] sums;
    }

    numOfSums = toMove.numOfSums;
    sums = toMove.sums;
    hasResults = toMove.hasResults;
    toMove.sums = nullptr;

    return *this;
}
JoinSumResult::~JoinSumResult() {
    if (sums != nullptr) {
        delete[] sums;
    }
}

uint32_t JoinSumResult::getNumOfSums() const {
    return numOfSums;
}
uint64_t JoinSumResult::getSum(const uint32_t sumNum) const {
    if (sumNum >= numOfSums) {
        throw runtime_error("getSum: out of bounds [sumNum="
                            + to_string(sumNum)
                            + ", numOfSums="
                            + to_string(numOfSums)
                            + "]");
    }
    return sums[sumNum];
}
uint64_t JoinSumResult::addSum(const uint32_t sumNum, const uint64_t toAdd) {
    if (sumNum >= numOfSums) {
        throw runtime_error("getSum: out of bounds [sumNum="
                            + to_string(sumNum)
                            + ", numOfSums="
                            + to_string(numOfSums)
                            + "]");
    }
    return sums[sumNum] += toAdd;
}

bool JoinSumResult::getHasResults() const {
    return hasResults;
}

void JoinSumResult::setHasResults() {
    hasResults = true;
}

ostream& operator<<(ostream& os, const JoinSumResult& toPrint) {
    os << "[JoinSumResult numOfSums=" << toPrint.numOfSums << ", sums=";
    if (toPrint.sums == nullptr) {
        os << "null";
    }
    else {
        os << "[";
        for (uint32_t i = 0; i < toPrint.numOfSums; ++i) {
            if (i != 0) {
                os << ", ";
            }
            os << toPrint.sums[i];
        }
        os << "]";
    }
    os << ", hasResults=" << toPrint.hasResults << "]";
    return os;
}

