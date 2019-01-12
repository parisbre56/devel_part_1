/*
 * FilterRange.cpp
 *
 *  Created on: 9 Δεκ 2018
 *      Author: parisbre56
 */

#include "FilterRange.h"
#include "FilterGreater.h"
#include "FilterLesser.h"

using namespace std;

FilterRange::FilterRange(uint32_t table,
                         size_t col,
                         uint64_t minValueExclusive,
                         uint64_t maxValueExclusive) :
        Filter(table, col),
        minValueExclusive(minValueExclusive),
        maxValueExclusive(maxValueExclusive) {

}

FilterRange::~FilterRange() {
    //Do nothing
}

bool FilterRange::passesFilter(const Table& table, uint64_t rownum) const {
    return minValueExclusive < table.getValue(rownum, col)
           && table.getValue(rownum, col) < maxValueExclusive;
}

MultipleColumnStats FilterRange::applyFilter(const MultipleColumnStats& stat) const {
    return stat.filterRangeExclusive(col, minValueExclusive, maxValueExclusive);
}

Filter* FilterRange::mergeIfPossible(const Filter* const toMergeWith) const {
    //Only matching table/col
    if (toMergeWith->getTable() != table || toMergeWith->getCol() != col) {
        return nullptr;
    }
    {
        const FilterGreater* const filterGreater = dynamic_cast<const FilterGreater*>(toMergeWith);
        if (filterGreater != nullptr) {
            return new FilterRange(table,
                                   col,
                                   (filterGreater->getValue()
                                    > minValueExclusive) ? filterGreater->getValue() :
                                                           minValueExclusive,
                                   maxValueExclusive);
        }
    }
    {
        const FilterLesser* const filterLesser = dynamic_cast<const FilterLesser*>(toMergeWith);
        if (filterLesser != nullptr) {
            return new FilterRange(table,
                                    col,
                                   minValueExclusive,
                                   (filterLesser->getValue() < maxValueExclusive) ? filterLesser->getValue() :
                                                                                    maxValueExclusive);
        }
    }
    {
        const FilterRange* const filterRange = dynamic_cast<const FilterRange*>(toMergeWith);
        if (filterRange != nullptr) {
            return new FilterRange(table,
                                   col,
                                   (filterRange->getMinValueExclusive()
                                    > minValueExclusive) ? filterRange->getMinValueExclusive() :
                                                           minValueExclusive,
                                   (filterRange->getMaxValueExclusive()
                                    < maxValueExclusive) ? filterRange->getMaxValueExclusive() :
                                                           maxValueExclusive);
        }
    }
    //Can't merge with other filters
    return nullptr;
}

void FilterRange::write(ostream& os) const {
    os << "[FilterLesser table="
       << table
       << ", col="
       << col
       << ", minValueExclusive="
       << minValueExclusive
       << ", maxValueExclusive="
       << maxValueExclusive
       << "]";
}

uint64_t FilterRange::getMinValueExclusive() const {
    return minValueExclusive;
}

uint64_t FilterRange::getMaxValueExclusive() const {
    return maxValueExclusive;
}

std::ostream& operator<<(std::ostream& os, const FilterRange& toPrint) {
    toPrint.write(os);
    return os;
}
