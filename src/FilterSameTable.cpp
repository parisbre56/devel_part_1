/*
 * FilterSameTable.cpp
 *
 *  Created on: 9 Δεκ 2018
 *      Author: parisbre56
 */

#include "FilterSameTable.h"

using namespace std;

FilterSameTable::FilterSameTable(uint32_t table, size_t col, size_t colB) :
        Filter(table, col), colB(colB) {

}

FilterSameTable::~FilterSameTable() {
    //Do nothing
}

size_t FilterSameTable::getColB() const {
    return colB;
}

bool FilterSameTable::passesFilter(const Table& table, uint64_t rownum) const {
    if (col == colB) {
        return true; //We don't have the possibility of null, so this is always true
    }
    return table.getValue(rownum, col) == table.getValue(rownum, colB);
}

MultipleColumnStats FilterSameTable::applyFilter(const MultipleColumnStats& stat) const {
    return stat.filterSame(col, colB);
}

Filter* FilterSameTable::mergeIfPossible(const Filter* const toMergeWith) const {
    //Never merge
    return nullptr;
}

void FilterSameTable::write(ostream& os) const {
    os
    << "[FilterLesser table="
    << table
    << ", col="
    << col
    << ", colB="
    << colB
    << "]";
}


std::ostream& operator<<(std::ostream& os, const FilterSameTable& toPrint) {
    toPrint.write(os);
    return os;
}
