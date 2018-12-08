/*
 * FilterLesser.h
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#ifndef FILTERLESSER_H_
#define FILTERLESSER_H_
class FilterLesser;

#include "Filter.h"

class FilterLesser: public Filter {
    virtual void write(std::ostream& os) const;
public:
    FilterLesser() = delete;
    FilterLesser(uint32_t table, size_t col, uint64_t value);
    FilterLesser(const FilterLesser& toCopy) = delete;
    FilterLesser(FilterLesser&& toMove) = delete;
    FilterLesser& operator=(const FilterLesser& toCopy) = delete;
    FilterLesser& operator=(FilterLesser&& toMove) = delete;
    virtual ~FilterLesser();

    virtual bool passesFilter(uint64_t value) const;

    friend std::ostream& operator<<(std::ostream& os,
                                    const FilterLesser& toPrint);
};

#endif /* FILTERLESSER_H_ */
