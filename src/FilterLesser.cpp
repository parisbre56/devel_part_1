/*
 * FilterLesser.cpp
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#include "FilterLesser.h"

using namespace std;

FilterLesser::FilterLesser(uint32_t table, size_t col, uint64_t value) :
        Filter(table, col, value) {

}

bool FilterLesser::passesFilter(uint64_t value) const {
    return value < this->value;
}

void FilterLesser::write(ostream& os) const {
    os << "[FilterLesser table="
       << table
       << ", col="
       << col
       << ", value="
       << value
       << "]";
}

FilterLesser::~FilterLesser() {
    //Do nothing
}

std::ostream& operator<<(std::ostream& os, const FilterLesser& toPrint) {
    toPrint.write(os);
    return os;
}
