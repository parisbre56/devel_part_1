/*
 * ColumnStat.cpp
 *
 *  Created on: 17 Δεκ 2018
 *      Author: parisbre56
 */

#include "ColumnStat.h"

#include <limits>

using namespace std;

ColumnStat::ColumnStat() :
        minVal(numeric_limits<uint64_t>::max()),
        maxVal(0),
        totalRows(0),
        uniqueValues(0) {

}

ColumnStat::ColumnStat(uint64_t minVal,
                       uint64_t maxVal,
                       uint64_t totalRows,
                       uint64_t uniqueValues) :
        minVal(minVal),
        maxVal(maxVal),
        totalRows(totalRows),
        uniqueValues(uniqueValues) {

}

uint64_t ColumnStat::getMaxVal() const {
    return maxVal;
}

void ColumnStat::setMaxVal(uint64_t maxVal) {
    this->maxVal = maxVal;
}

uint64_t ColumnStat::getMinVal() const {
    return minVal;
}

void ColumnStat::setMinVal(uint64_t minVal) {
    this->minVal = minVal;
}

uint64_t ColumnStat::getTotalRows() const {
    return totalRows;
}

void ColumnStat::setTotalRows(uint64_t totalRows) {
    this->totalRows = totalRows;
}

uint64_t ColumnStat::getUniqueValues() const {
    return uniqueValues;
}

void ColumnStat::setUniqueValues(uint64_t uniqueValues) {
    this->uniqueValues = uniqueValues;
}

void ColumnStat::incrementUniqueValues() {
    ++uniqueValues;
}

ostream& operator<<(ostream& os, const ColumnStat& toPrint) {
    os << "[ColumnStat minVal="
       << toPrint.minVal
       << ", maxVal="
       << toPrint.maxVal
       << ", totalRows="
       << toPrint.totalRows
       << ", uniqueValues="
       << toPrint.uniqueValues
       << "]";
    return os;
}
