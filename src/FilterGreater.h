/*
 * FilterGreater.h
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#ifndef FILTERGREATER_H_
#define FILTERGREATER_H_

class FilterGreater;

#include "FilterNumericValue.h"
#include "MultipleColumnStats.h"

class FilterGreater: public FilterNumericValue {
    virtual void write(std::ostream& os) const;
public:
    FilterGreater() = delete;
    FilterGreater(uint32_t table, size_t col, uint64_t value);
    FilterGreater(const FilterGreater& toCopy) = delete;
    FilterGreater(FilterGreater&& toMove) = delete;
    FilterGreater& operator=(const FilterGreater& toCopy) = delete;
    FilterGreater& operator=(FilterGreater&& toMove) = delete;
    virtual ~FilterGreater();

    virtual bool passesFilter(const Table& table, uint64_t rownum) const;
    MultipleColumnStats applyFilter(const MultipleColumnStats& stat) const;
    Filter* mergeIfPossible(const Filter* const toMergeWith) const;

    friend std::ostream& operator<<(std::ostream& os,
                                    const FilterGreater& toPrint);
};

#endif /* FILTERGREATER_H_ */
