/*
 * ResultContainer.cpp
 *
 *  Created on: 29 Οκτ 2018
 *      Author: pibre
 */

#include "ResultContainer.h"

#include <cstring>

#include <sstream>
#include <limits>

#include "ConsoleOutput.h"

using namespace std;

ResultContainer::ResultContainer(uint64_t maxExpectedResults,
                                 uint32_t sizeTableRows,
                                 size_t sizePayloads,
                                 bool* usedRows) :
        sizeTableRows(sizeTableRows),
        sizePayloads(sizePayloads),
        resultCount(0),
        usedRows(
                (usedRows == nullptr) ? (new bool[sizeTableRows] { /*init to false*/}) :
                                        (usedRows)),
        manageUsedRows(usedRows == nullptr) {
    const size_t sizeOfTuple = sizeof(Tuple)
                               + sizeof(Tuple*)
                               + sizeTableRows * sizeof(uint64_t)
                               + sizePayloads * sizeof(uint64_t);
    const uint64_t blockSize = (RESULTCONTAINER_H_L2_CACHE_SIZE
                                - sizeof(Result)
                                - sizeof(Relation))
                               / (sizeOfTuple);
    start = new Result((maxExpectedResults < blockSize) ? (maxExpectedResults) :
                                                          (blockSize),
                       sizeTableRows,
                       sizePayloads,
                       this->usedRows);
    end = start;
}

ResultContainer::ResultContainer(const ResultContainer& toCopy) :
        sizeTableRows(toCopy.sizeTableRows),
        sizePayloads(toCopy.sizePayloads),
        resultCount(toCopy.resultCount),
        usedRows(
                toCopy.manageUsedRows ? new bool[toCopy.sizeTableRows] :
                                        toCopy.usedRows),
        manageUsedRows(toCopy.manageUsedRows) {
    if (manageUsedRows) {
        memcpy(usedRows, toCopy.usedRows, sizeTableRows * sizeof(bool));
    }
    start = new Result(*(toCopy.start), usedRows);
    end = start->getFirstNonFullSegment();
}

ResultContainer::ResultContainer(ResultContainer&& toMove) :
        sizeTableRows(toMove.sizeTableRows),
        sizePayloads(toMove.sizePayloads),
        resultCount(toMove.resultCount),
        usedRows(toMove.usedRows),
        manageUsedRows(toMove.manageUsedRows),
        start(toMove.start),
        end(toMove.end) {
    toMove.usedRows = nullptr;
    toMove.start = nullptr;
    toMove.end = nullptr;
}

ResultContainer& ResultContainer::operator=(const ResultContainer& toCopy) {
    if (this == &toCopy) {
        return *this;
    }
    if (start != nullptr) {
        delete start;
    }
    if (manageUsedRows && usedRows != nullptr) {
        delete[] usedRows;
    }
    sizeTableRows = toCopy.sizeTableRows;
    sizePayloads = toCopy.sizePayloads;
    resultCount = toCopy.resultCount;
    if (toCopy.manageUsedRows) {
        usedRows = new bool[toCopy.sizeTableRows];
        memcpy(usedRows, toCopy.usedRows, sizeTableRows * sizeof(bool));
    }
    else {
        usedRows = toCopy.usedRows;
    }
    manageUsedRows = toCopy.manageUsedRows;
    start = new Result(*(toCopy.start), usedRows);
    end = start->getFirstNonFullSegment();
    return *this;
}

ResultContainer& ResultContainer::operator=(ResultContainer&& toMove) {
    if (this == &toMove) {
        return *this;
    }
    if (start != nullptr) {
        delete start;
    }
    if (manageUsedRows && usedRows != nullptr) {
        delete[] usedRows;
    }
    sizeTableRows = toMove.sizeTableRows;
    sizePayloads = toMove.sizePayloads;
    resultCount = toMove.resultCount;
    usedRows = toMove.usedRows;
    manageUsedRows = toMove.manageUsedRows;
    start = toMove.start;
    end = toMove.end;
    toMove.start = nullptr;
    toMove.end = nullptr;
    toMove.usedRows = nullptr;
    return *this;
}
ResultContainer::~ResultContainer() {
    if (start != nullptr) {
        delete start;
    }
    if (manageUsedRows && usedRows != nullptr) {
        delete[] usedRows;
    }
}

const Result* ResultContainer::getFirstResultBlock() const {
    return start;
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

void ResultContainer::addTuple(Tuple&& toAdd) {
    if (resultCount != numeric_limits<uint64_t>::max()) {
        ++resultCount;
    }
    Result* nextEnd = end->addTuple(move(toAdd));
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

const bool * ResultContainer::getUsedRows() const {
    return usedRows;
}

void ResultContainer::setUsedRow(uint32_t row) {
    if (row >= sizeTableRows) {
        throw runtime_error("ResultContainer::setUsedRow: index out of bounds [row="
                            + to_string(row)
                            + ", sizeTableRows="
                            + to_string(sizeTableRows)
                            + "]");
    }
    if (!manageUsedRows) {
        throw runtime_error("ResultContainer::setUsedRow: manageUsedRows is false");
    }
    usedRows[row] = true;
}

Relation ResultContainer::loadToRelation(const size_t sizePayloads,
                                         const uint64_t * const * const payloadCols,
                                         const uint32_t * const payloadTables) const {
    ConsoleOutput consoleOutput("ResultContainer::loadToRelation");
    CO_IFDEBUG(consoleOutput,
               "ResultContainer::loadToRelation [sizePayloads="<<sizePayloads<<", payloadTables=[");
    if (payloadTables == nullptr) {
        CO_IFDEBUG(consoleOutput, "null");
    }
    else {
        for (size_t i = 0; i < sizePayloads; ++i) {
            CO_IFDEBUG(consoleOutput, "\t" << payloadTables[i])
        }
    }
    CO_IFDEBUG(consoleOutput, "]");
    Relation rel(resultCount, sizeTableRows, sizePayloads, usedRows);
    const Result* currResult = start;
    while (currResult != nullptr) {
        const Relation& currRelation = currResult->getRelation();
        const uint32_t numTuples = currRelation.getNumTuples();
        const Tuple * const * currTuple = currRelation.getTuples();
        for (uint32_t i = 0; i < numTuples; ++i, ++currTuple) {
            Tuple toAdd(**currTuple, sizePayloads);
            for (size_t j = 0; j < sizePayloads; ++j) {
                toAdd.setPayload(j,
                                 payloadCols[j][toAdd.getTableRow(payloadTables[j])]);
            }
            CO_IFDEBUG(consoleOutput, "Adding Tuple "<<toAdd);
            rel.addTuple(move(toAdd));
        }
        currResult = currResult->getNext();
    }
    return rel;
}

void ResultContainer::mergeResult(ResultContainer&& toMerge) {
    Result* oldEndNext = end->getNext();
    end->setNext(toMerge.start);
    toMerge.end->getLastSegment()->setNext(oldEndNext);
    toMerge.start = nullptr;
}

std::ostream& operator<<(std::ostream& os, const ResultContainer& toPrint) {
    os << "[ResultContainer sizeTableRows="
       << toPrint.sizeTableRows
       << ", sizePayloads="
       << toPrint.sizePayloads
       << ", resultCount="
       << toPrint.resultCount
       << ", usedRows=";
    if (toPrint.usedRows == nullptr) {
        os << "null";
    }
    else {
        os << "[";
        for (uint32_t i = 0; i < toPrint.sizeTableRows; ++i) {
            if (i != 0) {
                os << ", ";
            }
            os << toPrint.usedRows[i];
        }
        os << "]";
    }
    os << ", manageUsedRows=" << toPrint.manageUsedRows << ", start=";
    if (toPrint.start == nullptr) {
        os << "null";
    }
    else {
        os << *(toPrint.start);
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

