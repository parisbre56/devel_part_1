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
    tuples = new Tuple[this->arraySize];
}

Relation::Relation(uint64_t numOfTuplesToCopy,
                   Tuple * const tuplesToCopy) {
    numTuples = numOfTuplesToCopy;
    arraySize = numOfTuplesToCopy;
    tuples = new const Tuple*[numOfTuplesToCopy];
    for (uint64_t i = 0; i < numOfTuplesToCopy; ++i) {
        tuples[i] = tuplesToCopy[i];
    }
}

Relation::Relation(const Relation& toCopy) :
        Relation(toCopy.numTuples, toCopy.tuples) {
}

Relation::Relation(Relation&& toMove) {
    numTuples = toMove.numTuples;
    arraySize = toMove.arraySize;
    tuples = toMove.tuples;
    toMove.tuples = nullptr;
}

Relation::~Relation() {
    //Delete all copies if this hasn't been moved
    if (tuples != nullptr) {
        delete[] tuples;
    }
}

uint64_t Relation::getNumTuples() const {
    return numTuples;
}

void Relation::addTuple(Tuple& tuple) {
    if (numTuples >= arraySize) {
        throw runtime_error("addTuple: reached limit, can't add more tuples [num_tuples="
                            + to_string(numTuples)
                            + ", array_size="
                            + to_string(arraySize)
                            + "]");
    }

    //Add copy
    tuples[numTuples++] = tuple;
}

void Relation::addTuple(uint64_t key, uint64_t payload) {
    if (numTuples >= arraySize) {
        throw runtime_error("addTuple: reached limit, can't add more tuples [num_tuples="
                            + to_string(numTuples)
                            + ", array_size="
                            + to_string(arraySize)
                            + "]");
    }

    //Add copy
    Tuple& toAdd = tuples[numTuples++];
    toAdd.setKey(key);
    toAdd.setPayload(payload);
}

const Tuple& Relation::getTuple(uint64_t index) const {
    if (index >= numTuples) {
        throw runtime_error("index out of bounds [index="
                            + to_string(index)
                            + ", num_tuples="
                            + to_string(numTuples)
                            + "]");
    }

    return tuples[index];
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
            os << "\n\t" << i << ": " << toPrint.tuples[i];
        }
        os << "]";
    }
    os << "]";
    return os;
}
