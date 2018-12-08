/*
 * Result.cpp
 *
 *  Created on: 21 Γ�Γ�Γ΄ 2018
 *      Author: parisbre56
 */

#include "Result.h"

#include <sstream>
#include <stdexcept>

#include <cstring>

using namespace std;

Result::Result(uint64_t blockSize,
               int32_t sizeTableRows,
               size_t sizePayloads,
               bool* usedRows) :
        sizeTableRows(sizeTableRows),
        sizePayloads(sizePayloads),
        usedRows(usedRows),
        relation(new Relation(blockSize,
                              sizeTableRows,
                              sizePayloads,
                              usedRows)),
        next(nullptr) {

}

Result::Result(const Result& toCopy, bool * usedRows) :
        sizeTableRows(toCopy.sizeTableRows),
        sizePayloads(toCopy.sizePayloads),
        usedRows(usedRows),
        relation(new Relation(*(toCopy.relation), usedRows)),
        next(toCopy.next == nullptr ? nullptr :
                                      new Result(*(toCopy.next), usedRows)) { //TODO can do better performance by eliminating recursion
}

Result::Result(Result&& toMove) :
        sizeTableRows(toMove.sizeTableRows),
        sizePayloads(toMove.sizePayloads),
        usedRows(toMove.usedRows),
        relation(toMove.relation),
        next(toMove.next) {
    toMove.relation = nullptr;
    toMove.next = nullptr;
}

Result::~Result() {
    if (relation != nullptr) {
        delete relation;
    }
    //Don't recurse deletion for better performance
    while (next != nullptr) {
        Result* toDelete = next;
        next = toDelete->next;
        toDelete->next = nullptr;
        delete toDelete;
    }
}
/** The tuples in this segment **/
const Relation& Result::getRelation() const {
    return *relation;
}

Result* Result::getNext() const {
    return next;
}

Result* Result::getLastSegment() {
    Result* retVal = this;
    while (retVal->next != nullptr) {
        retVal = retVal->next;
    }
    return retVal;
}

Result* Result::getFirstNonFullSegment() {
    Result* retVal = this;
    while (retVal->relation->getNumTuples() == retVal->relation->getArraySize()
           && retVal->next != nullptr) {
        retVal = retVal->next;
    }
    return retVal;
}

void Result::getLastSegment(Result*& lastSegment, Result*& createdSegment) {
    lastSegment = this;
    createdSegment = nullptr;
    if (lastSegment->relation->getNumTuples()
        == lastSegment->relation->getArraySize()) {
        lastSegment = getFirstNonFullSegment();
        if (lastSegment->relation->getNumTuples()
            == lastSegment->relation->getArraySize()) {
            lastSegment->next = new Result(relation->getArraySize(),
                                           sizeTableRows,
                                           sizePayloads,
                                           usedRows);
            lastSegment = lastSegment->next;
        }
        createdSegment = lastSegment;
    }
}

Result* Result::addTuple(Tuple& toAdd) {
    Result* lastSegment;
    Result* retVal;
    getLastSegment(lastSegment, retVal);
    lastSegment->relation->addTuple(toAdd);
    return retVal;
}

Result* Result::addTuple(Tuple&& toAdd) {
    Result* lastSegment;
    Result* retVal;
    getLastSegment(lastSegment, retVal);
    lastSegment->relation->addTuple(move(toAdd));
    return retVal;
}

void Result::reset() {
    Result* currResult = this;
    do {
        currResult->relation->reset();
    } while (currResult != nullptr);
}

std::ostream& operator<<(std::ostream& os, const Result& toPrint) {
    os << "[Result sizeTableRows="
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
    os << "], relation=" << toPrint.relation << ", next=";
    if (toPrint.next == nullptr) {
        os << "null";
    }
    else {
        os << (*(toPrint.next));
    }
    os << "]";
    return os;
}
