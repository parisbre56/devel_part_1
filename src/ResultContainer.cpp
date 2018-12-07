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

ResultContainer::ResultContainer(uint64_t blockSize,
                                 uint32_t sizeTableRows,
                                 size_t sizePayloads,
                                 bool* usedRows = nullptr) :
        sizeTableRows(sizeTableRows),
        sizePayloads(sizePayloads),
        resultCount(0),
        usedRows(
                usedRows == nullptr ? new bool[sizeTableRows] { /*init to false*/} :
                                      usedRows),
        manageUsedRows(usedRows == nullptr),
        end(nullptr) {
    start = new Result(blockSize, sizeTableRows, sizePayloads, this->usedRows);
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

void ResultContainer::loadToRelation(Relation& rel,
                                     const uint32_t * const payloadTables,
                                     const uint64_t * const * const payloadCols) const {
    const Result* currResult = start;
    uint64_t rowKey = 0;
    size_t sizePayloads = rel.getSizePayloads();
    while (currResult != nullptr) {
        const Relation& currRelation = currResult->getRelation();
        const uint32_t numTuples = currRelation.getNumTuples();
        const Tuple * currTuple = currRelation.getTuples();
        for (uint32_t i = 0; i < numTuples; ++i, ++currTuple) {
            Tuple toAdd(*currTuple, sizePayloads);
            const uint64_t * const * currCol = payloadCols;
            const uint32_t * currPayloadTable = payloadTables;
            for (size_t j = 0; j < sizePayloads;
                    ++j, ++currCol, ++payloadTables) {
                toAdd.setPayload(j,
                                 (*currCol)[toAdd.getTableRow(*currPayloadTable)]);
            }
            rel.addTuple(move(toAdd));
        }
        currResult = start->next;
    }
}

std::ostream& operator<<(std::ostream& os, const ResultContainer& toPrint) {
    todo
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

