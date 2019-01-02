/*
 * NumericValueFilter.h
 *
 *  Created on: 9 Δεκ 2018
 *      Author: parisbre56
 */

#ifndef FILTERNUMERICVALUE_H_
#define FILTERNUMERICVALUE_H_
class FilterNumericValue;

#include "Filter.h"

class FilterNumericValue: public Filter {
protected:
    const uint64_t value;

    virtual void write(std::ostream& os) const = 0;
public:
    FilterNumericValue() = delete;
    FilterNumericValue(uint32_t table, size_t col, uint64_t value);
    FilterNumericValue(const FilterNumericValue& toCopy) = delete;
    FilterNumericValue(FilterNumericValue&& toMove) = delete;
    FilterNumericValue& operator=(const FilterNumericValue& toCopy) = delete;
    FilterNumericValue& operator=(FilterNumericValue&& toMove) = delete;
    virtual ~FilterNumericValue();

    uint64_t getValue() const;
    virtual bool passesFilter(const Table& table, uint64_t rownum) const = 0;
    virtual MultipleColumnStats applyFilter(const MultipleColumnStats& stat) const = 0;

    friend std::ostream& operator<<(std::ostream& os,
                                    const FilterNumericValue& toPrint);
};

#endif /* FILTERNUMERICVALUE_H_ */
