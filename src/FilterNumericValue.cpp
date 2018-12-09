/*
 * NumericValueFilter.cpp
 *
 *  Created on: 9 Δεκ 2018
 *      Author: parisbre56
 */

#include "FilterNumericValue.h"

FilterNumericValue::FilterNumericValue(uint32_t table,
                                       size_t col,
                                       uint64_t value) :
        Filter(table, col), value(value) {

}

FilterNumericValue::~FilterNumericValue() {
    //Do nothing
}

uint64_t FilterNumericValue::getValue() const {
    return value;
}

std::ostream& operator<<(std::ostream& os, const FilterNumericValue& toPrint) {
    toPrint.write(os);
    return os;
}

