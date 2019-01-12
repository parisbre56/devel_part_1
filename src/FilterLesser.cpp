/*
 * FilterLesser.cpp
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#include "FilterLesser.h"
#include "FilterGreater.h"
#include "FilterRange.h"

using namespace std;

FilterLesser::FilterLesser(uint32_t table, size_t col, uint64_t value) :
        FilterNumericValue(table, col, value) {

}

bool FilterLesser::passesFilter(const Table& table, uint64_t rownum) const {
    return table.getValue(rownum, col) < value;
}

MultipleColumnStats FilterLesser::applyFilter(const MultipleColumnStats& stat) const {
    return stat.filterRangeLesser(col, value);
}

Filter* FilterLesser::mergeIfPossible(const Filter* const toMergeWith) const {
    //Only matching table/col
    if (toMergeWith->getTable() != table || toMergeWith->getCol() != col) {
        return nullptr;
    }
    {
        const FilterGreater* const filterGreater = dynamic_cast<const FilterGreater*>(toMergeWith);
        if (filterGreater != nullptr) {
            return new FilterRange(table, col, filterGreater->getValue(), value);
        }
    }
    {
        const FilterLesser* const filterLesser = dynamic_cast<const FilterLesser*>(toMergeWith);
        if (filterLesser != nullptr) {
            return new FilterLesser(table,
                                    col,
                                    (filterLesser->value < value) ? filterLesser->value :
                                                                    value);
        }
    }
    {
        const FilterRange* const filterRange = dynamic_cast<const FilterRange*>(toMergeWith);
        if (filterRange != nullptr) {
            return new FilterRange(table,
                                   col,
                                   filterRange->getMinValueExclusive(),
                                   (filterRange->getMaxValueExclusive() < value) ? filterRange->getMaxValueExclusive() :
                                                                                   value);
        }
    }
    //Can't merge with other filters
    return nullptr;
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
