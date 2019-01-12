/*
 * FilterGreater.cpp
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#include "FilterGreater.h"
#include "FilterLesser.h"
#include "FilterRange.h"

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

Filter* FilterGreater::mergeIfPossible(const Filter* const toMergeWith) const {
    //Only matching table/col
    if (toMergeWith->getTable() != table || toMergeWith->getCol() != col) {
        return nullptr;
    }
    {
        const FilterGreater* const filterGreater = dynamic_cast<const FilterGreater*>(toMergeWith);
        if (filterGreater != nullptr) {
            return new FilterGreater(table,
                                     col,
                                     (filterGreater->value > value) ? filterGreater->value :
                                                                      value);
        }
    }
    {
        const FilterLesser* const filterLesser = dynamic_cast<const FilterLesser*>(toMergeWith);
        if (filterLesser != nullptr) {
            return new FilterRange(table, col, value, filterLesser->getValue());
        }
    }
    {
        const FilterRange* const filterRange = dynamic_cast<const FilterRange*>(toMergeWith);
        if (filterRange != nullptr) {
            return new FilterRange(table,
                                   col,
                                   (filterRange->getMinValueExclusive() > value) ? filterRange->getMinValueExclusive() :
                                                                                   value,
                                   filterRange->getMaxValueExclusive());
        }
    }
    //Can't merge with other filters
    return nullptr;
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
