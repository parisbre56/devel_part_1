/*
 * FilterSameTable.h
 *
 *  Created on: 9 Δεκ 2018
 *      Author: parisbre56
 */

#ifndef FILTERSAMETABLE_H_
#define FILTERSAMETABLE_H_
class FilterSameTable;

#include "Filter.h"

class FilterSameTable: public Filter {
protected:
    size_t colB;

    void write(std::ostream& os) const;
public:
    FilterSameTable() = delete;
    FilterSameTable(uint32_t table, size_t col, size_t colB);
    virtual ~FilterSameTable();

    FilterSameTable(const FilterSameTable& toCopy) = delete;
    FilterSameTable(FilterSameTable&& toMove) = delete;
    FilterSameTable& operator=(const FilterSameTable& toCopy) = delete;
    FilterSameTable& operator=(FilterSameTable&& toMove) = delete;

    size_t getColB() const;

    bool passesFilter(const Table& table, uint64_t rownum) const;
    MultipleColumnStats applyFilter(const MultipleColumnStats& stat) const;
    Filter* mergeIfPossible(const Filter* const toMergeWith) const;

    friend std::ostream& operator<<(std::ostream& os,
                                    const FilterSameTable& toPrint);
};

#endif /* FILTERSAMETABLE_H_ */
