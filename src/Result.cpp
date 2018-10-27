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

Result::Result() {
    tuples = new Tuple[RESULT_H_BLOCK_SIZE];
    numTuples = 0;
    next = nullptr;
}

Result::Result(const Result& toCopy) {
    tuples = new Tuple[RESULT_H_BLOCK_SIZE];
    copyValuesInternal(toCopy);
}

Result& Result::operator=(const Result& toCopy) {
    copyValuesInternal(toCopy);
    return *this;
}

void Result::copyValuesInternal(const Result& toCopy) {
    Result* ptSelf = this;
    const Result* ptOther = &toCopy;

    //Copy all segments
    while (true) {
        ptSelf->numTuples = ptOther->numTuples;
        //If there are no data in this segment, there's no reason to continue,
        //we're certain there won't be any data in the next segments
        if (ptOther->numTuples == 0) {
            break;
        }
        //Copy data to the new segment
        memcpy(ptSelf->tuples,
               ptOther->tuples,
               sizeof(Tuple) * ptOther->numTuples);

        //If there is no next segment in the other, break
        ptOther = ptOther->next;
        if (ptOther == nullptr) {
            break;
        }

        //Else, If there is a next segment in the other, create
        //a next segment for self if necessary
        if (ptSelf->next == nullptr) {
            ptSelf->next = new Result();
        }
        ptSelf = ptSelf->next;
    }
    //Empty out all remaining segments in self
    ptSelf = ptSelf->next;
    while (ptSelf != nullptr && ptSelf->numTuples > 0) {
        ptSelf = ptSelf->next;
        ptSelf->numTuples = 0;
    }
}

Result::Result(Result&& toMove) {
    moveValuesInternal(toMove);
}

Result& Result::operator=(Result&& toMove) {
    if (tuples != nullptr) {
        delete tuples;
    }
    if (next != nullptr) {
        delete next;
    }
    moveValuesInternal(toMove);
    return *this;
}

void Result::moveValuesInternal(Result& toMove) {
    numTuples = toMove.numTuples;
    tuples = toMove.tuples;
    next = toMove.next;
    toMove.tuples = nullptr;
    toMove.next = nullptr;
}

Result::~Result() {
    if (tuples != nullptr) {
        delete[] tuples;
    }
    //Don't recurse deletion for better performance
    while (next != nullptr) {
        Result* toDelete = next;
        next = toDelete->next;
        toDelete->next = nullptr;
        delete toDelete;
    }
}

uint32_t Result::getNumTuples() const {
    return numTuples;
}

Result* Result::getLastSegment() {
    Result* retVal = this;
    while (retVal->next != nullptr) {
        retVal = retVal->next;
    }
    return retVal;
}

void Result::addTuple(Tuple& toAdd) {
    Result* lastSegment = getLastSegment();
    if (lastSegment->numTuples == RESULT_H_BLOCK_SIZE) {
        lastSegment->next = new Result();
        lastSegment = lastSegment->next;
    }
    lastSegment->tuples[lastSegment->numTuples] = toAdd;
    lastSegment->numTuples++;
}

std::ostream& operator<<(std::ostream& os, const Result& toPrint) {
    os << "[Result numTuples=" << toPrint.numTuples << ", tuples=";
    if (toPrint.tuples == nullptr) {
        os << "null";
    }
    else {
        os << "[";
        for (uint32_t i = 0; i < toPrint.numTuples; ++i) {
            os << "\n\t" << i << ": " << toPrint.tuples[i];
        }
        os << "]";
    }
    os << ", next=";
    if (toPrint.next == nullptr) {
        os << "null";
    }
    else {
        os << (*(toPrint.next));
    }
    os << "]";
    return os;
}
