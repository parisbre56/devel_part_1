/*
 * Relation.cpp
 *
 *  Created on: 21 Γ�Γ�Γ΄ 2018
 *      Author: parisbre56
 */

#include "Relation.h"

#include <sstream>
#include <stdexcept>

using namespace std;

Relation::Relation(uint64_t arraySize, uint64_t arrayIncrementSize) {
    numTuples = 0;
    this->arraySize = arraySize;
    tuples = new const Tuple*[this->arraySize];
    this->arrayIncrementSize = arrayIncrementSize;
}

Relation::Relation(uint64_t numOfTuplesToCopy,
                   const Tuple* const * const tuplesToCopy,
                   uint64_t arrayIncrementSizeToCopy) {
    numTuples = numOfTuplesToCopy;
    arraySize = numOfTuplesToCopy;
    tuples = new const Tuple*[numOfTuplesToCopy];
    for (uint64_t i = 0; i < numOfTuplesToCopy; ++i) {
        tuples[i] = new const Tuple(*(tuplesToCopy[i]));
    }
    arrayIncrementSize = arrayIncrementSizeToCopy;
}

Relation::Relation(const Relation& toCopy) :
        Relation(toCopy.numTuples, toCopy.tuples, toCopy.arrayIncrementSize) {
}

Relation::Relation(Relation&& toMove) {
    numTuples = toMove.numTuples;
    arraySize = toMove.arraySize;
    arrayIncrementSize = toMove.arrayIncrementSize;
    tuples = toMove.tuples;
    toMove.tuples = nullptr;
}

Relation::~Relation() {
    //Delete all copies if this hasn't been moved
    if (tuples != nullptr) {
        for (uint64_t i = 0; i < numTuples; ++i) {
            delete tuples[i];
        }
        delete[] tuples;
    }
}

uint64_t Relation::getNumTuples() const {
    return numTuples;
}

void Relation::addTuple(Tuple& tuple) {
    if (numTuples > arraySize) {
        throw runtime_error("num_tuples was greater than array_size (should never happen) [num_tuples="
                            + to_string(numTuples)
                            + ", array_size="
                            + to_string(arraySize)
                            + "]");
    }

    //If we reached the array limit
    if (numTuples == arraySize) {
        //Create copy with new size
        uint64_t new_array_size = arraySize + arrayIncrementSize;
        const Tuple** new_tuples = new const Tuple*[new_array_size];

        //Copy old to new
        copy(tuples, tuples + arraySize, new_tuples);

        //Delete old
        delete tuples;

        //Update metadata
        tuples = new_tuples;
        arraySize = new_array_size;
    }

    //Add copy
    tuples[numTuples] = new Tuple(tuple);
    ++numTuples;
}

const Tuple& Relation::getTuple(uint64_t index) const {
    if (index >= numTuples) {
        throw runtime_error("index out of bounds [index="
                            + to_string(index)
                            + ", num_tuples="
                            + to_string(numTuples)
                            + "]");
    }

    return *(tuples[index]);
}

std::ostream& operator<<(std::ostream& os, const Relation& toPrint) {
    os << "[Relation array_size="
       << toPrint.arraySize
       << ", num_tuples="
       << toPrint.numTuples
       << ", tuples=";
    if (toPrint.tuples == nullptr) {
        os << "null";
    }
    else {
        os << "[";
        for (uint64_t i = 0; i < toPrint.numTuples; ++i) {
            os << "\n\t" << i << ": " << (*(toPrint.tuples[i]));
        }
        os << "]";
    }
    os << "]";
    return os;
}
