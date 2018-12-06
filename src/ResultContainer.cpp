/*
 * ResultContainer.cpp
 *
 *  Created on: 29 Οκτ 2018
 *      Author: pibre
 */

#include "ResultContainer.h"

#include <sstream>
#include <limits>

#include "ConsoleOutput.h"

using namespace std;

ResultContainer::ResultContainer() {
    start = new Result();
    end = start;
    resultCount = 0;
}

ResultContainer::ResultContainer(const ResultContainer& toCopy) {
    start = new Result(*(toCopy.start));
    end = start->getFirstNonFullSegment();
    resultCount = toCopy.resultCount;
}

ResultContainer::ResultContainer(ResultContainer&& toMove) {
    start = toMove.start;
    end = toMove.end;
    resultCount = toMove.resultCount;
    toMove.start = nullptr;
    toMove.end = nullptr;
}

ResultContainer& ResultContainer::operator=(const ResultContainer& toCopy) {
    if (start != nullptr) {
        delete start;
    }
    start = new Result(*(toCopy.start));
    end = start->getFirstNonFullSegment();
    resultCount = toCopy.resultCount;
    return *this;
}

ResultContainer& ResultContainer::operator=(ResultContainer&& toMove) {
    if (start != nullptr) {
        delete start;
    }
    start = toMove.start;
    end = toMove.end;
    resultCount = toMove.resultCount;
    toMove.start = nullptr;
    toMove.end = nullptr;
    return *this;
}
ResultContainer::~ResultContainer() {
    if (start != nullptr) {
        delete start;
    }
}

void ResultContainer::addTuple(Tuple& toAdd) {
    if (resultCount != numeric_limits<uint64_t>::max()) {
        ++resultCount;
    }
    Result* nextEnd = end->addTuple(toAdd);
    if (nextEnd != nullptr) {
        end = nextEnd;
    }
}

void ResultContainer::reset() {
    resultCount = 0;
    start->reset();
    end = start;
}

uint64_t ResultContainer::getResultCount() const {
    return resultCount;
}

void ResultContainer::loadToRelation(Relation& rel,
                                     const uint64_t * const currCol,
                                     const bool useKey) const {
    const Result* currResult = start;
    uint64_t rowKey = 0;
    while (currResult != nullptr) {
        const uint32_t numTuples = currResult->getNumTuples();
        const Tuple * currTuple = currResult->getTuples();
        for (uint32_t i = 0; i < numTuples; ++i, ++currTuple) {
            const uint64_t rowNum =
                    (useKey) ? (currTuple->getKey()) :
                               (currTuple->getPayload());
            rel.addTuple(rowNum, currCol[rowNum]);
        }
        currResult = start->next;
    }
}

std::ostream& operator<<(std::ostream& os, const ResultContainer& toPrint) {
    os << "[ResultContainer start=";
    if (toPrint.start == nullptr) {
        os << "null";
    }
    else {
        os << toPrint.start;
    }
    os << ", end=";
    if (toPrint.end == nullptr) {
        os << "null";
    }
    else {
        os << toPrint.end;
    }
    os << "]";
    return os;
}

