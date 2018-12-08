/*
 * FilterEquals.h
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#ifndef FILTEREQUALS_H_
#define FILTEREQUALS_H_
class FilterEquals;

#include "Filter.h"

class FilterEquals: public Filter {
    virtual void write(std::ostream& os) const;
public:
    FilterEquals() = delete;
    FilterEquals(uint32_t table, size_t col, uint64_t value);
    FilterEquals(const FilterEquals& toCopy) = delete;
    FilterEquals(FilterEquals&& toMove) = delete;
    FilterEquals& operator=(const FilterEquals& toCopy) = delete;
    FilterEquals& operator=(FilterEquals&& toMove) = delete;
    virtual ~FilterEquals();

    virtual bool passesFilter(uint64_t value) const;

    friend std::ostream& operator<<(std::ostream& os,
                                    const FilterEquals& toPrint);
};

#endif /* FILTEREQUALS_H_ */
