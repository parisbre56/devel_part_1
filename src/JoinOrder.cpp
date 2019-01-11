/*
 * JoinTree.cpp
 *
 *  Created on: 27 Δεκ 2018
 *      Author: parisbre56
 */

#include "JoinOrder.h"

#include <cstring>

#include <sstream>

using namespace std;

JoinOrder::JoinOrder(uint32_t tables) :
        arraySize(tables),
        orderedTables(0),
        tableOrder(new uint32_t[tables]),
        stats(new const MultipleColumnStats*[tables]),
        rowSum(new double[tables]) {
}

JoinOrder::JoinOrder(uint32_t tables,
                     uint32_t table,
                     const MultipleColumnStats& tableStats) :
        JoinOrder(tables) {
    addTable(table, tableStats);
}

JoinOrder::JoinOrder(const JoinOrder& toCopy) :
        arraySize(toCopy.arraySize),
        orderedTables(toCopy.orderedTables),
        tableOrder(new uint32_t[toCopy.arraySize]),
        stats(new const MultipleColumnStats*[toCopy.arraySize]),
        rowSum(new double[toCopy.arraySize]) {
    memcpy(tableOrder,
           toCopy.tableOrder,
           toCopy.orderedTables * sizeof(uint32_t));
    memcpy(rowSum, toCopy.rowSum, toCopy.orderedTables * sizeof(double));
    for (uint32_t i = 0; i < toCopy.orderedTables; ++i) {
        stats[i] = new MultipleColumnStats(*(toCopy.stats[i]));
    }
}
JoinOrder::JoinOrder(JoinOrder&& toMove) :
        arraySize(toMove.arraySize),
        orderedTables(toMove.orderedTables),
        tableOrder(toMove.tableOrder),
        stats(toMove.stats),
        rowSum(toMove.rowSum) {
    toMove.tableOrder = nullptr;
    toMove.stats = nullptr;
    toMove.rowSum = nullptr;
}
JoinOrder& JoinOrder::operator=(const JoinOrder& toCopy) {
    if (arraySize != toCopy.arraySize) {
        throw runtime_error("JoinOrder::operator= array size does not match [arraySize="
                            + to_string(arraySize)
                            + ", toCopy.arraySize="
                            + to_string(toCopy.arraySize)
                            + "]");
    }
    for (uint32_t i = 0; i < orderedTables; ++i) {
        delete stats[i];
    }

    orderedTables = toCopy.orderedTables;
    memcpy(tableOrder,
           toCopy.tableOrder,
           toCopy.orderedTables * sizeof(uint32_t));
    memcpy(rowSum, toCopy.rowSum, toCopy.orderedTables * sizeof(double));
    for (uint32_t i = 0; i < toCopy.orderedTables; ++i) {
        stats[i] = new MultipleColumnStats(*(toCopy.stats[i]));
    }
    return *this;
}
JoinOrder& JoinOrder::operator=(JoinOrder&& toMove) {
    if (tableOrder != nullptr) {
        delete[] tableOrder;
    }
    if (stats != nullptr) {
        for (uint32_t i = 0; i < orderedTables; ++i) {
            delete stats[i];
        }
        delete[] stats;
    }
    arraySize = toMove.arraySize;
    orderedTables = toMove.orderedTables;
    tableOrder = toMove.tableOrder;
    toMove.tableOrder = nullptr;
    stats = toMove.stats;
    toMove.stats = nullptr;
    rowSum = toMove.rowSum;
    toMove.rowSum = nullptr;
    return *this;
}

JoinOrder::~JoinOrder() {
    if (tableOrder != nullptr) {
        delete[] tableOrder;
    }
    if (stats != nullptr) {
        for (uint32_t i = 0; i < orderedTables; ++i) {
            delete stats[i];
        }
        delete[] stats;
    }
    if (rowSum != nullptr) {
        delete[] rowSum;
    }
}

void JoinOrder::addTable(uint32_t table,
                         const MultipleColumnStats& tableStats) {
    if (containsTable(table)) {
        stringstream errString;
        errString << "JoinOrder::addTable already contains table [table="
                  << table
                  << ", joinOrder="
                  << *this
                  << "]";
        throw runtime_error(errString.str());
    }
    if (orderedTables >= arraySize) {
        stringstream errString;
        errString << "JoinOrder::addTable reached limit, cannot add more ordered tables [table="
                  << table
                  << ", joinOrder="
                  << *this
                  << "]";
        throw runtime_error(errString.str());
    }
    tableOrder[orderedTables] = table;
    stats[orderedTables] = new MultipleColumnStats(tableStats);
    rowSum[orderedTables] =
            (orderedTables == 0) ? tableStats.getColumnStats()[0].getTotalRows() :
                                   tableStats.getColumnStats()[0].getTotalRows()
                                   + rowSum[orderedTables - 1];
    orderedTables++;
}

JoinOrder JoinOrder::addTableNew(uint32_t table,
                                 const MultipleColumnStats& tableStats) const {
    JoinOrder retVal(*this);
    retVal.addTable(table, tableStats);
    return retVal;
}

bool JoinOrder::containsTable(uint32_t table) const {
    for (uint32_t i = 0; i < orderedTables; ++i) {
        if (tableOrder[i] == table) {
            return true;
        }
    }
    return false;
}

bool JoinOrder::sameSet(const JoinOrder& other) const {
    //We obviously need the two sets to be of the same size if we want them to be the same set
    if (orderedTables != other.orderedTables) {
        return false;
    }
    //Since we're certain both sets have the same size and have no duplicates, then we can be
    //certain if the other contains all of this, then this contains all of other
    for (uint32_t i = 0; i < orderedTables; ++i) {
        if (!other.containsTable(tableOrder[i])) {
            return false;
        }
    }
    return true;
}

uint32_t JoinOrder::getArraySize() const {
    return arraySize;
}

uint32_t JoinOrder::getOrderedTables() const {
    return orderedTables;
}

const uint32_t* JoinOrder::getTableOrder() const {
    return tableOrder;
}

const MultipleColumnStats* const * JoinOrder::getStats() const {
    return stats;
}

const double* JoinOrder::getRowSum() const {
    return rowSum;
}

ostream& operator<<(ostream& os, const JoinOrder& toPrint) {
    os << "[JoinOrder arraySize="
       << toPrint.arraySize
       << ", orderedTables="
       << toPrint.orderedTables
       << ", tableOrder=";
    if (toPrint.tableOrder == nullptr) {
        os << "null";
    }
    else {
        os << "[";
        for (uint32_t i = 0; i < toPrint.orderedTables; ++i) {
            if (i != 0) {
                os << ", ";
            }
            os << toPrint.tableOrder[i];
        }
        os << "]";
    }
    return os;
}
