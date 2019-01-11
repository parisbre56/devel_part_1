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
        totalRows(0.0),
        uniqueValues(0.0) {

}

ColumnStat::ColumnStat(uint64_t minVal,
                       uint64_t maxVal,
                       double totalRows,
                       double uniqueValues) :
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

double ColumnStat::getTotalRows() const {
    return totalRows;
}

void ColumnStat::setTotalRows(double totalRows) {
    this->totalRows = totalRows;
}

double ColumnStat::getUniqueValues() const {
    return uniqueValues;
}

void ColumnStat::setUniqueValues(double uniqueValues) {
    this->uniqueValues = uniqueValues;
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
