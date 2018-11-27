/*
 * Filter.cpp
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#include "Filter.h"

Filter::Filter(uint32_t table, size_t col, uint64_t value) :
        table(table), col(col), value(value) {

}

uint32_t Filter::getTable() const {
    return table;
}

size_t Filter::getCol() const {
    return col;
}

Filter::~Filter() {
    //Do nothing
}

std::ostream& operator<<(std::ostream& os, const Filter& toPrint) {
    toPrint.write(os);
    return os;
}
