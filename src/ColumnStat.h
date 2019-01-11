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
    double totalRows;
    double uniqueValues;
public:
    ColumnStat();
    ColumnStat(uint64_t minVal,
               uint64_t maxVal,
               double totalRows,
               double uniqueValues);
    /* Use trivial copy/move constructors */

    uint64_t getMaxVal() const;
    void setMaxVal(uint64_t maxVal);
    uint64_t getMinVal() const;
    void setMinVal(uint64_t minVal);
    double getTotalRows() const;
    void setTotalRows(double totalRows);
    double getUniqueValues() const;
    void setUniqueValues(double uniqueValues);

    friend std::ostream& operator<<(std::ostream& os,
                                    const ColumnStat& toPrint);
};

#endif /* COLUMNSTAT_H_ */
