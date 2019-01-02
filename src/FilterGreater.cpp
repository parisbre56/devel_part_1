/*
 * FilterGreater.cpp
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#include "FilterGreater.h"

using namespace std;

FilterGreater::FilterGreater(uint32_t table, size_t col, uint64_t value) :
        FilterNumericValue(table, col, value) {

}

bool FilterGreater::passesFilter(const Table& table, uint64_t rownum) const {
    return table.getValue(rownum, col) > value;
}

MultipleColumnStats FilterGreater::applyFilter(const MultipleColumnStats& stat) const {
    return stat.filterRangeGreater(col, value);
}

void FilterGreater::write(ostream& os) const {
    os << "[FilterGreater table="
       << table
       << ", col="
       << col
       << ", value="
       << value
       << "]";
}

FilterGreater::~FilterGreater() {
    //Do nothing
}

std::ostream& operator<<(std::ostream& os, const FilterGreater& toPrint) {
    toPrint.write(os);
    return os;
}
