/*
 * Filter.h
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#ifndef FILTER_H_
#define FILTER_H_
class Filter;

#include <cstdint>

#include <string>
#include <iostream>

class Filter {
protected:
    const uint32_t table;
    const size_t col;
    const uint64_t value;

    virtual void write(std::ostream& os) const = 0;
public:
    Filter() = delete;
    Filter(uint32_t table, size_t col, uint64_t value);
    Filter(const Filter& toCopy) = delete;
    Filter(Filter&& toMove) = delete;
    Filter& operator=(const Filter& toCopy) = delete;
    Filter& operator=(Filter&& toMove) = delete;
    virtual ~Filter();

    /** True if value should be kept, false if value should be discarded **/
    virtual bool passesFilter(uint64_t value) const = 0;
    uint32_t getTable() const;
    size_t getCol() const;

    friend std::ostream& operator<<(std::ostream& os, const Filter& toPrint);
};

#endif /* FILTER_H_ */
