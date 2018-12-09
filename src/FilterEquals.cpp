/*
 * FilterEquals.cpp
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#include "FilterEquals.h"

using namespace std;

FilterEquals::FilterEquals(uint32_t table, size_t col, uint64_t value) :
        FilterNumericValue(table, col, value) {

}

bool FilterEquals::passesFilter(const Table& table, uint64_t rownum) const {
    return table.getValue(rownum, col) == value;
}

void FilterEquals::write(ostream& os) const {
    os << "[FilterEquals table="
       << table
       << ", col="
       << col
       << ", value="
       << value
       << "]";
}

FilterEquals::~FilterEquals() {
    //Do nothing
}

std::ostream& operator<<(std::ostream& os, const FilterEquals& toPrint) {
    toPrint.write(os);
    return os;
}
