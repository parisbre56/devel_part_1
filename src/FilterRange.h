/*
 * FilterRange.h
 *
 *  Created on: 9 Δεκ 2018
 *      Author: parisbre56
 */

#ifndef FILTERRANGE_H_
#define FILTERRANGE_H_
class FilterRange;

#include "Filter.h"

class FilterRange: public Filter {
protected:
    uint64_t minValueExclusive;
    uint64_t maxValueExclusive;

    void write(std::ostream& os) const;
public:
    FilterRange() = delete;
    FilterRange(uint32_t table,
                size_t col,
                uint64_t minValueExclusive,
                uint64_t maxValueExclusive);
    virtual ~FilterRange();

    FilterRange(const FilterRange& toCopy) = delete;
    FilterRange(FilterRange&& toMove) = delete;
    FilterRange& operator=(const FilterRange& toCopy) = delete;
    FilterRange& operator=(FilterRange&& toMove) = delete;

    uint64_t getMinValueExclusive() const;
    uint64_t getMaxValueExclusive() const;

    bool passesFilter(const Table& table, uint64_t rownum) const;
    MultipleColumnStats applyFilter(const MultipleColumnStats& stat) const;
    Filter* mergeIfPossible(const Filter* const toMergeWith) const;

    friend std::ostream& operator<<(std::ostream& os,
                                    const FilterRange& toPrint);
};

#endif /* FILTERRANGE_H_ */
