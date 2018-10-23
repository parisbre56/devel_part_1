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
    numTuples = 0;
    next = nullptr;
}

Result::Result(const Result& toCopy) {
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

Result::~Result() {
    if (next != nullptr) {
        delete next;
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
    if (lastSegment->numTuples == BLOCK_SIZE) {
        lastSegment->next = new Result();
        lastSegment = lastSegment->next;
    }
    lastSegment->tuples[lastSegment->numTuples] = toAdd;
    lastSegment->numTuples++;
}

string Result::toString() const {
    ostringstream retVal;
    retVal
    << "[Result numTuples=" << numTuples
    << ", tuples=[";
    for (uint32_t i = 0; i < numTuples; ++i) {
        retVal << "\n\t" << i << ": " << tuples[i].toString();
    }
    retVal << "], next="
           << (next == nullptr ? "null" : next->toString())
           << "]";
    return retVal.str();
}
