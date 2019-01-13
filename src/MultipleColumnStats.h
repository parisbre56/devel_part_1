/*
 * ColumnStats.h
 *
 *  Created on: 17 Δεκ 2018
 *      Author: parisbre56
 */

#ifndef MULTIPLECOLUMNSTATS_H_
#define MULTIPLECOLUMNSTATS_H_
class MultipleColumnStats;

#include <ostream>

#include "ColumnStat.h"
#include "Table.h"

#define MULTIPLECOLUMNSTATS_H_MAX_UNIQUE_SIZE 49999999
#define MULTIPLECOLUMNSTATS_H_STACK_ARRAY_SIZE 500000

class MultipleColumnStats {
protected:
    size_t cols;
    ColumnStat* columnStats;

    void setUniqueValues(const Table& table,
                         const uint64_t rows,
                         uint64_t maxLength,
                         bool* encountered);

public:
    explicit MultipleColumnStats(const Table& table,
                                 const std::string tempFile);
    MultipleColumnStats(const MultipleColumnStats& toCopy);
    MultipleColumnStats(const MultipleColumnStats& toCopyLeft,
                        const MultipleColumnStats& toCopyRight);
    MultipleColumnStats(MultipleColumnStats&& toMove);
    MultipleColumnStats& operator=(const MultipleColumnStats& toCopy);
    MultipleColumnStats& operator=(MultipleColumnStats&& toMove);
    virtual ~MultipleColumnStats();

    MultipleColumnStats filterEquals(size_t col, uint64_t value) const;
    MultipleColumnStats filterRangeGreater(const size_t col,
                                           const uint64_t greaterThan) const;
    MultipleColumnStats filterRangeLesser(const size_t col,
                                          const uint64_t lessThan) const;
    MultipleColumnStats filterRangeExclusive(const size_t col,
                                             const uint64_t minValueExclusive,
                                             const uint64_t maxValueExclusive) const;
    MultipleColumnStats filterRange(const size_t col,
                                    const uint64_t greaterOrEqualTo,
                                    const uint64_t lessThanOrEqualTo) const;
    MultipleColumnStats filterSame(size_t colA, size_t colB) const;
    MultipleColumnStats join(size_t colThis,
                             const MultipleColumnStats& other,
                             size_t colOther) const;

    size_t getCols() const;
    const ColumnStat* getColumnStats() const;

    friend std::ostream& operator<<(std::ostream& os,
                                    const MultipleColumnStats& toPrint);
};

#endif /* MULTIPLECOLUMNSTATS_H_ */
