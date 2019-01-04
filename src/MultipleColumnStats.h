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

#define MULTIPLECOLUMNSTATS_H_MAX_UNIQUE_SIZE 40000000

class MultipleColumnStats {
protected:
    size_t cols;
    ColumnStat* columnStats;

    void updateOtherRows(size_t col,
                         const MultipleColumnStats& current,
                         ColumnStat* retValColumnStats,
                         ColumnStat& colStatNew,
                         const ColumnStat& colStat) const;
public:
    explicit MultipleColumnStats(const Table& table);
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
    MultipleColumnStats filterRange(size_t col,
                                    uint64_t greaterOrEqualTo,
                                    uint64_t lessThanOrEqualTo) const;
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
