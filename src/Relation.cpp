/*
 * Relation.cpp
 *
 *  Created on: 21 Γ�Γ�Γ΄ 2018
 *      Author: parisbre56
 */

#include "Relation.h"

#include <cstring>

#include <sstream>
#include <stdexcept>

using namespace std;

Relation::Relation(uint64_t arraySize,
                   uint32_t sizeTableRows,
                   size_t sizePayloads,
                   bool* usedRows) :
        numTuples(0),
        arraySize(arraySize),
        tuples(new Tuple*[arraySize]),
        usedRows(
                usedRows == nullptr ? new bool[sizeTableRows] { /*init to false*/} :
                                      usedRows),
        manageUsedRows(usedRows == nullptr),
        sizeTableRows(sizeTableRows),
        sizePayloads(sizePayloads) {
}

Relation::Relation(const Relation& toCopy) :
        numTuples(toCopy.numTuples),
        arraySize(toCopy.arraySize),
        tuples(new Tuple*[toCopy.arraySize]),
        usedRows(new bool[toCopy.sizeTableRows]),
        manageUsedRows(true),
        sizeTableRows(toCopy.sizeTableRows),
        sizePayloads(toCopy.sizePayloads) {
    memcpy(usedRows, toCopy.usedRows, toCopy.sizeTableRows * sizeof(bool));
    for (uint64_t i = 0; i < toCopy.arraySize; ++i) {
        tuples[i] = new Tuple(*(toCopy.tuples[i]));
    }
}

Relation::Relation(const Relation& toCopy, bool * const usedRows) :
        numTuples(toCopy.numTuples),
        arraySize(toCopy.arraySize),
        tuples(new Tuple*[toCopy.arraySize]),
        usedRows(usedRows),
        manageUsedRows(false),
        sizeTableRows(toCopy.sizeTableRows),
        sizePayloads(toCopy.sizePayloads) {
    for (uint64_t i = 0; i < toCopy.arraySize; ++i) {
        tuples[i] = new Tuple(*(toCopy.tuples[i]));
    }
}

Relation::Relation(Relation&& toMove) :
        numTuples(toMove.numTuples),
        arraySize(toMove.arraySize),
        tuples(toMove.tuples),
        usedRows(toMove.usedRows),
        manageUsedRows(toMove.manageUsedRows),
        sizeTableRows(toMove.sizeTableRows),
        sizePayloads(toMove.sizePayloads) {
    toMove.tuples = nullptr;
    toMove.usedRows = nullptr;
}

Relation::~Relation() {
    //Delete all copies if this hasn't been moved
    if (tuples != nullptr) {
        for (uint64_t i = 0; i < numTuples; ++i) {
            delete tuples[i];
        }
        delete[] tuples;
    }
    if (manageUsedRows && usedRows != nullptr) {
        delete[] usedRows;
    }
}

ResultContainer Relation::operator*(const Relation& cartesianProduct) const {
    ResultContainer retResult(numTuples * cartesianProduct.numTuples,
                              sizeTableRows,
                              0);
    for (uint64_t i = 0; i < numTuples; ++i) {
        for (uint64_t j = 0; j < cartesianProduct.numTuples; ++j) {
            const bool* const usedRows = retResult.getUsedRows();
            Tuple toAdd(*(tuples[i]),
                        usedRows,
                        *(cartesianProduct.tuples[j]),
                        usedRows);
            retResult.addTuple(move(toAdd));
        }
    }
    return retResult;
}

uint64_t Relation::getNumTuples() const {
    return numTuples;
}
uint64_t Relation::getArraySize() const {
    return arraySize;
}
const bool* Relation::getUsedRows() const {
    return usedRows;
}
bool Relation::getUsedRow(uint32_t row) const {
    if (row >= sizeTableRows) {
        throw runtime_error("Relation::getUsedRow: index out of bounds [row="
                            + to_string(row)
                            + ", sizeTableRows="
                            + to_string(sizeTableRows)
                            + "]");
    }
    return usedRows[row];
}
void Relation::setUsedRow(uint32_t row) {
    if (row >= sizeTableRows) {
        throw runtime_error("Relation::setUsedRow: index out of bounds [row="
                            + to_string(row)
                            + ", sizeTableRows="
                            + to_string(sizeTableRows)
                            + "]");
    }
    if (!manageUsedRows) {
        throw runtime_error("Relation::setUsedRow: manageUsedRows is false");
    }
    usedRows[row] = true;
}
uint32_t Relation::getSizeTableRows() const {
    return sizeTableRows;
}
size_t Relation::getSizePayloads() const {
    return sizePayloads;
}

/** Add a copy of the Tuple to the array of tuples,
 * incrementing the size of the underlying array
 * automatically if necessary.
 *
 * The Relation will delete the created Tuple when it
 * is deleted. **/
void Relation::addTuple(Tuple& tuple) {
    if (numTuples >= arraySize) {
        throw runtime_error("Relation::addTuple: reached limit, can't add more tuples [numTuples="
                            + to_string(numTuples)
                            + ", arraySize="
                            + to_string(arraySize)
                            + "]");
    }
    tuples[numTuples++] = new Tuple(tuple);
}
void Relation::addTuple(Tuple&& tuple) {
    if (numTuples >= arraySize) {
        throw runtime_error("Relation::addTuple: reached limit, can't add more tuples [numTuples="
                            + to_string(numTuples)
                            + ", arraySize="
                            + to_string(arraySize)
                            + "]");
    }
    tuples[numTuples++] = new Tuple(move(tuple));
}
/** Get the tuple at the given index. **/
const Tuple* const * const Relation::getTuples() const {
    return tuples;
}
const Tuple& Relation::getTuple(uint64_t index) const {
    return *(tuples[index]);
}

void Relation::reset() { //TODO can be made more efficient by reusing tuples
    for (uint64_t i = 0; i < numTuples; ++i) {
        delete tuples[i];
    }
    numTuples = 0;
}

/*numTuples(toCopy.numTuples),
 arraySize(toCopy.arraySize),
 tuples(new Tuple*[toCopy.arraySize]),
 usedRows(new bool[toCopy.sizeTableRows]),
 manageUsedRows(true),
 sizeTableRows(toCopy.sizeTableRows),
 sizePayloads(toCopy.sizePayloads)*/

std::ostream& operator<<(std::ostream& os, const Relation& toPrint) {
    os << "[Relation array_size="
       << toPrint.arraySize
       << ", num_tuples="
       << toPrint.numTuples
       << ", sizeTableRows="
       << toPrint.sizeTableRows
       << ", sizePayloads="
       << toPrint.sizePayloads
       << ", manageUsedRows="
       << toPrint.manageUsedRows
       << ", usedRows=";
    if (toPrint.usedRows == nullptr) {
        os << "null";
    }
    else {
        os << "[";
        for (size_t i = 0; i < toPrint.sizeTableRows; ++i) {
            if (i != 0) {
                os << ", ";
            }
            os << toPrint.usedRows[i];
        }
        os << "]";
    }
    os << ", tuples=";
    if (toPrint.tuples == nullptr) {
        os << "null";
    }
    else {
        os << "[";
        for (uint64_t i = 0; i < toPrint.numTuples; ++i) {
            os << "\n\t" << i << ": " << *(toPrint.tuples[i]);
        }
        os << "]";
    }
    os << "]";
    return os;
}
