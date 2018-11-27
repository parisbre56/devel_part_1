/*
 * FilterGreater.cpp
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#include "FilterGreater.h"

using namespace std;

FilterGreater::FilterGreater(uint32_t table, size_t col, uint64_t value) :
        Filter(table, col, value) {

}

bool FilterGreater::passesFilter(uint64_t value) const {
    return value > this->value;
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
