/*
 * FilterGreater.h
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#ifndef FILTERGREATER_H_
#define FILTERGREATER_H_

#include "Filter.h"

class FilterGreater: public Filter {
    virtual void write(std::ostream& os) const;
public:
    FilterGreater() = delete;
    FilterGreater(uint32_t table, size_t col, uint64_t value);
    FilterGreater(const FilterGreater& toCopy) = delete;
    FilterGreater(FilterGreater&& toMove) = delete;
    FilterGreater& operator=(const FilterGreater& toCopy) = delete;
    FilterGreater& operator=(FilterGreater&& toMove) = delete;
    virtual ~FilterGreater();

    virtual bool passesFilter(uint64_t value) const;

    friend std::ostream& operator<<(std::ostream& os,
                                    const FilterGreater& toPrint);
};

#endif /* FILTERGREATER_H_ */
