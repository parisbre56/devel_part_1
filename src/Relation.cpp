/*
 * Relation.cpp
 *
 *  Created on: 21 Γ�Γ�Γ΄ 2018
 *      Author: parisbre56
 */

#include "Relation.h"

#include <sstream>
#include <stdexcept>

#define DEFAULT_TUPLE_LENGTH 100
#define DEFAULT_INCREASE 100

using namespace std;

Relation::Relation() {
    numTuples = 0;
    arraySize = DEFAULT_TUPLE_LENGTH;
    tuples = new const Tuple*[DEFAULT_TUPLE_LENGTH];
}

Relation::Relation(uint32_t numOfTuplesToCopy,
                   const Tuple* const * const tuplesToCopy) {
    numTuples = numOfTuplesToCopy;
    arraySize = numOfTuplesToCopy;
    tuples = new const Tuple*[numOfTuplesToCopy];
    for (uint32_t i; i < numOfTuplesToCopy; ++i) {
        tuples[i] = new const Tuple(*(tuplesToCopy[i]));
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
        for (uint32_t i = 0; i < numTuples; ++i) {
            delete tuples[i];
        }
        delete tuples;
    }
}

uint32_t Relation::getNumTuples() const {
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
        uint32_t new_array_size = arraySize + DEFAULT_INCREASE;
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

const Tuple& Relation::getTuple(uint32_t index) const {
    if (index >= numTuples) {
        throw runtime_error("index out of bounds [index="
                            + to_string(index)
                            + ", num_tuples="
                            + to_string(numTuples)
                            + "]");
    }

    return *(tuples[index]);
}

string Relation::toString() const {
    ostringstream retVal;
    retVal << "[Relation array_size="
           << arraySize
           << ", num_tuples="
           << numTuples
           << ", tuples=";
    if (tuples == nullptr) {
        retVal << "null";
    }
    else {
        retVal << "[";
        for (uint32_t i = 0; i < numTuples; ++i) {
            retVal << "\n\t" << i << ": " << tuples[i]->toString();
        }
        retVal << "]";
    }
    retVal << "]";
    return retVal.str();
}
