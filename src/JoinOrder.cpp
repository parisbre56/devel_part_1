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
        arraySize(tables), orderedTables(0), tableOrder(new uint32_t[tables]) {
}

JoinOrder::JoinOrder(uint32_t tables, uint32_t table) :
        JoinOrder(tables) {
    addTable(table);
}

JoinOrder::JoinOrder(const JoinOrder& toCopy) :
        arraySize(toCopy.arraySize),
        orderedTables(toCopy.orderedTables),
        tableOrder(new uint32_t[toCopy.arraySize]) {
    memcpy(tableOrder,
           toCopy.tableOrder,
           toCopy.orderedTables * sizeof(uint32_t));
}
JoinOrder::JoinOrder(JoinOrder&& toMove) :
        arraySize(toMove.arraySize),
        orderedTables(toMove.orderedTables),
        tableOrder(toMove.tableOrder) {
    toMove.tableOrder = nullptr;
}
JoinOrder& JoinOrder::operator=(const JoinOrder& toCopy) {
    if (arraySize != toCopy.arraySize) {
        throw runtime_error("JoinOrder::operator= array size does not match [arraySize="
                            + to_string(arraySize)
                            + ", toCopy.arraySize="
                            + to_string(toCopy.arraySize)
                            + "]");
    }
    orderedTables = toCopy.orderedTables;
    memcpy(tableOrder,
           toCopy.tableOrder,
           toCopy.orderedTables * sizeof(uint32_t));
    return *this;
}
JoinOrder& JoinOrder::operator=(JoinOrder&& toMove) {
    if (tableOrder != nullptr) {
        delete[] tableOrder;
    }
    arraySize = toMove.arraySize;
    orderedTables = toMove.orderedTables;
    tableOrder = toMove.tableOrder;
    toMove.tableOrder = nullptr;
    return *this;
}

JoinOrder::~JoinOrder() {
    if (tableOrder != nullptr) {
        delete[] tableOrder;
    }
}

void JoinOrder::addTable(uint32_t table) {
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
    tableOrder[orderedTables++] = table;
}

JoinOrder JoinOrder::addTableNew(uint32_t table) const {
    JoinOrder retVal(*this);
    retVal.addTable(table);
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
