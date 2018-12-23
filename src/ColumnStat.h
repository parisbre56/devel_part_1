/*
 * ColumnStat.h
 *
 *  Created on: 17 Δεκ 2018
 *      Author: parisbre56
 */

#ifndef COLUMNSTAT_H_
#define COLUMNSTAT_H_
class ColumnStat;

#include <ostream>

#include "Table.h"

class ColumnStat {
protected:
    uint64_t minVal;
    uint64_t maxVal;
    uint64_t totalRows;
    uint64_t uniqueValues;
public:
    ColumnStat();
    ColumnStat(uint64_t minVal,
               uint64_t maxVal,
               uint64_t totalRows,
               uint64_t uniqueValues);
    /* Use trivial copy/move constructors */

    uint64_t getMaxVal() const;
    void setMaxVal(uint64_t maxVal);
    uint64_t getMinVal() const;
    void setMinVal(uint64_t minVal);
    uint64_t getTotalRows() const;
    void setTotalRows(uint64_t totalRows);
    uint64_t getUniqueValues() const;
    void setUniqueValues(uint64_t uniqueValues);
    void incrementUniqueValues();

    friend std::ostream& operator<<(std::ostream& os,
                                    const ColumnStat& toPrint);
};

#endif /* COLUMNSTAT_H_ */
