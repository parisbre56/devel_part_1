/*
 * Relation.cpp
 *
 *  Created on: 21 Γ�Γ�Γ΄ 2018
 *      Author: parisbre56
 */

#include "Relation.h"

#include <sstream>
#include <stdexcept>

#define DEFAULT_TUPLE_LENGTH 10
#define DEFAULT_INCREASE 10

using namespace std;

Relation::Relation() {
    num_tuples = 0;
    array_size = DEFAULT_TUPLE_LENGTH;
    tuples = new const Tuple*[DEFAULT_TUPLE_LENGTH];
}

Relation::Relation(uint32_t num_of_tuples_to_copy,
                   const Tuple* const * const tuples_to_copy) {
    num_tuples = num_of_tuples_to_copy;
    array_size = num_of_tuples_to_copy;
    tuples = new const Tuple*[num_of_tuples_to_copy];
    for (uint32_t i; i < num_of_tuples_to_copy; ++i) {
        tuples[i] = new const Tuple(*(tuples_to_copy[i]));
    }
}

Relation::Relation(const Relation& to_copy) :
        Relation(to_copy.num_tuples, to_copy.tuples) {
}

Relation::~Relation() {
    //Delete all copies
    for (uint32_t i = 0; i < num_tuples; ++i) {
        delete tuples[i];
    }
    delete tuples;
}

uint32_t Relation::getNumTuples() const {
    return num_tuples;
}

void Relation::addTuple(Tuple& tuple) {
    if (num_tuples > array_size) {
        throw runtime_error("num_tuples was greater than array_size [num_tuples="
                            + to_string(num_tuples)
                            + ", array_size="
                            + to_string(array_size)
                            + "]");
    }

    //If we reached the array limit
    if (num_tuples == array_size) {
        //Create copy with new size
        uint32_t new_array_size = array_size + DEFAULT_INCREASE;
        const Tuple** new_tuples = new const Tuple*[new_array_size];

        //Copy old to new
        copy(tuples, tuples + array_size, new_tuples);

        //Delete old
        delete tuples;

        //Update metadata
        tuples = new_tuples;
        array_size = new_array_size;
    }

    //Add copy
    tuples[num_tuples] = new Tuple(tuple);
    ++num_tuples;
}

const Tuple& Relation::getTuple(uint32_t index) const {
    if (index < 0 || index >= num_tuples) {
        throw runtime_error("index out of bounds [index="
                            + to_string(index)
                            + ", num_tuples="
                            + to_string(num_tuples)
                            + "]");
    }

    return *(tuples[index]);
}

string Relation::toString() const {
    ostringstream retVal;
    retVal << "[Relation array_size="
           << array_size
           << ", num_tuples="
           << num_tuples
           << ", tuples=[";
    for (uint32_t i = 0; i < num_tuples; ++i) {
        if (i != 0) {
            retVal << "\n";
        }
        retVal << "\t" << i << ": " << tuples[i]->toString();
    }
    retVal << "]]";
    return retVal.str();
}
