/*
 * ColumnStats.cpp
 *
 *  Created on: 17 Δεκ 2018
 *      Author: parisbre56
 */

#include "MultipleColumnStats.h"

#include <cstring>
#include <cmath>

#include <limits>

using namespace std;

void updateOtherRows(size_t col,
                     const MultipleColumnStats& retVal,
                     ColumnStat& colStatNew,
                     ColumnStat& colStat);

MultipleColumnStats::MultipleColumnStats(const Table& table) :
        cols(table.getCols()), columnStats(new ColumnStat[table.getCols()]) {
    const uint64_t rows = table.getRows();
    uint64_t maxLength = 0;
    for (size_t currColNum = 0; currColNum < cols; ++currColNum) {
        const uint64_t* const currCol = table.getCol(currColNum);
        ColumnStat& currStat = columnStats[currColNum];
        currStat.setTotalRows(rows);
        for (uint64_t i = 0; i < rows; ++i) {
            uint64_t currVal = currCol[i];
            if (currVal > currStat.getMaxVal()) {
                currStat.setMaxVal(currVal);
            }
            if (currVal < currStat.getMinVal()) {
                currStat.setMinVal(currVal);
            }
        }
        uint64_t length = currStat.getMaxVal() - currStat.getMinVal() + 1;
        if (length > maxLength) {
            maxLength = length;
        }
    }
    if (maxLength > MULTIPLECOLUMNSTATS_H_MAX_UNIQUE_SIZE) {
        maxLength = MULTIPLECOLUMNSTATS_H_MAX_UNIQUE_SIZE;
    }
    bool encountered[maxLength] {/* init to false */};
    for (size_t currColNum = 0; currColNum < cols; ++currColNum) {
        const uint64_t* const currCol = table.getCol(currColNum);
        ColumnStat& currStat = columnStats[currColNum];
        uint64_t minVal = currStat.getMinVal();
        uint64_t uniqueValues = 0;
        for (uint64_t i = 0; i < rows; ++i) {
            uint64_t encounteredIndex = currCol[i] - minVal;
            if (encounteredIndex >= maxLength) {
                uniqueValues++;
            }
            else if (!(encountered[encounteredIndex])) {
                encountered[encounteredIndex] = true;
                uniqueValues++;
            }
        }
        currStat.setUniqueValues(uniqueValues);
        uint64_t maxUsedRows = currStat.getMaxVal() - currStat.getMinVal() + 1;
        if (maxUsedRows > maxLength) {
            maxUsedRows = maxLength;
        }
        for (uint64_t i = 0; i < maxUsedRows; ++i) {
            encountered[i] = false;
        }
    }
}

MultipleColumnStats::MultipleColumnStats(const MultipleColumnStats& toCopy) :
        cols(toCopy.cols), columnStats(new ColumnStat[toCopy.cols]) {
    memcpy(columnStats, toCopy.columnStats, sizeof(ColumnStat) * cols);
}

MultipleColumnStats::MultipleColumnStats(const MultipleColumnStats& toCopyLeft,
                                         const MultipleColumnStats& toCopyRight) :
        cols(toCopyLeft.cols + toCopyRight.cols),
        columnStats(new ColumnStat[toCopyLeft.cols + toCopyRight.cols]) {
    memcpy(columnStats,
           toCopyLeft.columnStats,
           sizeof(ColumnStat) * toCopyLeft.cols);
    memcpy(columnStats + toCopyLeft.cols,
           toCopyRight.columnStats,
           sizeof(ColumnStat) * toCopyRight.cols);
}

MultipleColumnStats::MultipleColumnStats(MultipleColumnStats&& toMove) :
        cols(toMove.cols), columnStats(toMove.columnStats) {
    if (toMove.columnStats == nullptr) {
        throw runtime_error("stats have already been moved");
    }
    toMove.columnStats = nullptr;
}

MultipleColumnStats& MultipleColumnStats::operator=(const MultipleColumnStats& toCopy) {
    if (cols < toCopy.cols) {
        delete[] columnStats;
        columnStats = new ColumnStat[toCopy.cols];
    }
    cols = toCopy.cols;
    memcpy(columnStats, toCopy.columnStats, cols * sizeof(ColumnStat));
    return *this;
}

MultipleColumnStats& MultipleColumnStats::operator=(MultipleColumnStats&& toMove) {
    cols = toMove.cols;
    if (toMove.columnStats == nullptr) {
        throw runtime_error("stats have already been moved");
    }
    if (columnStats != nullptr) {
        delete[] columnStats;
    }
    columnStats = toMove.columnStats;
    toMove.columnStats = nullptr;
    return *this;
}

MultipleColumnStats::~MultipleColumnStats() {
    if (columnStats != nullptr) {
        delete[] columnStats;
    }
}

void MultipleColumnStats::updateOtherRows(size_t col,
                                          const MultipleColumnStats& current,
                                          ColumnStat* retValColumnStats,
                                          ColumnStat& colStatNew,
                                          const ColumnStat& colStat) const {
    double changeRatio = 1.0
                         - (((double) (colStatNew.getTotalRows()))
                            / ((double) (colStat.getTotalRows())));
    for (size_t i = 0; i < current.cols; ++i) {
        if (i == col) {
            continue;
        }
        const ColumnStat& currColStat = current.getColumnStats()[i];
        ColumnStat& currColStatNew = retValColumnStats[i];
        currColStatNew.setTotalRows(colStatNew.getTotalRows());
        currColStatNew.setUniqueValues(((double) (currColStat.getUniqueValues()))
                                       * (1.0
                                          - pow(changeRatio,
                                                ((double) (currColStat.getTotalRows()))
                                                / ((double) (currColStat.getUniqueValues())))));
    }
}

MultipleColumnStats MultipleColumnStats::filterEquals(size_t col,
                                                      uint64_t value) const {
    if (col >= cols) {
        throw runtime_error("MultipleColumnStats::filterEquals index out of bounds [col="
                            + to_string(col)
                            + ", cols="
                            + to_string(cols)
                            + "]");
    }

    MultipleColumnStats retVal(*this);
    const ColumnStat& colStat = getColumnStats()[col];
    ColumnStat& colStatNew = retVal.columnStats[col];
    colStatNew.setMinVal(value);
    colStatNew.setMaxVal(value);

    if (colStat.getTotalRows() == 0) {
        return retVal;
    }

    if (!(colStat.getMinVal() <= value && value <= colStat.getMaxVal())) {
        for (size_t i = 0; i < cols; ++i) {
            ColumnStat& currColStatNew = retVal.columnStats[i];
            currColStatNew.setTotalRows(0);
            currColStatNew.setUniqueValues(0);
        }
        return retVal;
    }

    //Else, if there is at least one result
    colStatNew.setUniqueValues(1);
    colStatNew.setTotalRows(((double) colStat.getTotalRows())
                            / ((double) colStat.getUniqueValues()));
    updateOtherRows(col, *this, retVal.columnStats, colStatNew, colStat);
    return retVal;
}

MultipleColumnStats MultipleColumnStats::filterRangeGreater(const size_t col,
                                                            const uint64_t greaterThan) const {
    if (col >= cols) {
        throw runtime_error("MultipleColumnStats::greaterFilterRange index out of bounds [col="
                            + to_string(col)
                            + ", cols="
                            + to_string(cols)
                            + ", greaterThan="
                            + to_string(greaterThan)
                            + "]");
    }
    //We can be certain there are no results greater than the max value
    if (greaterThan == numeric_limits<uint64_t>::max()) {
        MultipleColumnStats retVal(*this);
        for (size_t i = 0; i < cols; ++i) {
            ColumnStat& currColStatNew = retVal.columnStats[i];
            currColStatNew.setTotalRows(0);
            currColStatNew.setUniqueValues(0);
        }
        return retVal;
    }
    return filterRange(col, greaterThan + 1, getColumnStats()[col].getMaxVal());
}

MultipleColumnStats MultipleColumnStats::filterRangeLesser(const size_t col,
                                                           const uint64_t lessThan) const {
    if (col >= cols) {
        throw runtime_error("MultipleColumnStats::lesserFilterRange index out of bounds [col="
                            + to_string(col)
                            + ", cols="
                            + to_string(cols)
                            + ", lessThan="
                            + to_string(lessThan)
                            + "]");
    }
    //We can be certain there are no values smaller than 0 (all unsigned)
    if (lessThan == 0) {
        MultipleColumnStats retVal(*this);
        for (size_t i = 0; i < cols; ++i) {
            ColumnStat& currColStatNew = retVal.columnStats[i];
            currColStatNew.setTotalRows(0);
            currColStatNew.setUniqueValues(0);
        }
        return retVal;
    }
    return filterRange(col, getColumnStats()[col].getMinVal(), lessThan - 1);
}

MultipleColumnStats MultipleColumnStats::filterRangeExclusive(const size_t col,
                                                              const uint64_t minValueExclusive,
                                                              const uint64_t maxValueExclusive) const {
    if (col >= cols) {
        throw runtime_error("MultipleColumnStats::filterRangeExclusive index out of bounds [col="
                            + to_string(col)
                            + ", cols="
                            + to_string(cols)
                            + "]");
    }
    if (maxValueExclusive == 0
        || minValueExclusive == numeric_limits<uint64_t>::max()) {
        MultipleColumnStats retVal(*this);
        for (size_t i = 0; i < cols; ++i) {
            ColumnStat& currColStatNew = retVal.columnStats[i];
            currColStatNew.setTotalRows(0);
            currColStatNew.setUniqueValues(0);
        }
        return retVal;
    }
    return filterRange(col, minValueExclusive + 1, maxValueExclusive - 1);
}

MultipleColumnStats MultipleColumnStats::filterRange(const size_t col,
                                                     const uint64_t greaterOrEqualTo,
                                                     const uint64_t lessThanOrEqualTo) const {
    if (greaterOrEqualTo == lessThanOrEqualTo) {
        return filterEquals(col, greaterOrEqualTo);
    }
    if (col >= cols) {
        throw runtime_error("MultipleColumnStats::filterRange index out of bounds [col="
                            + to_string(col)
                            + ", cols="
                            + to_string(cols)
                            + "]");
    }

    MultipleColumnStats retVal(*this);
    const ColumnStat& colStat = getColumnStats()[col];
    ColumnStat& colStatNew = retVal.columnStats[col];

    if (colStat.getTotalRows() == 0) {
        return retVal;
    }

    if (colStat.getMinVal() > lessThanOrEqualTo
        || colStat.getMaxVal() < greaterOrEqualTo
        || lessThanOrEqualTo < greaterOrEqualTo) {
        for (size_t i = 0; i < cols; ++i) {
            ColumnStat& currColStatNew = retVal.columnStats[i];
            currColStatNew.setTotalRows(0);
            currColStatNew.setUniqueValues(0);
        }
        return retVal;
    }

    //Else, if this has at least one result
    //adjust the max values
    if (colStatNew.getMaxVal() > lessThanOrEqualTo) {
        colStatNew.setMaxVal(lessThanOrEqualTo);
    }
    if (colStatNew.getMinVal() < greaterOrEqualTo) {
        colStatNew.setMinVal(greaterOrEqualTo);
    }
    //If min and max remain the same, then everything else remains the same
    if (colStatNew.getMinVal() == colStat.getMinVal()
        && colStatNew.getMaxVal() == colStat.getMaxVal()) {
        return retVal;
    }
    double ratio = ((double) (colStatNew.getMaxVal() - colStatNew.getMinVal()))
                   / ((double) (colStat.getMaxVal() - colStat.getMinVal()));
    colStatNew.setUniqueValues(ratio * colStat.getUniqueValues());
    colStatNew.setTotalRows(ratio * colStat.getTotalRows());
    updateOtherRows(col, *this, retVal.columnStats, colStatNew, colStat);
    return retVal;
}
MultipleColumnStats MultipleColumnStats::filterSame(size_t colA,
                                                    size_t colB) const {
    if (colA >= cols || colB >= cols) {
        throw runtime_error("MultipleColumnStats::filterRange index out of bounds [colA="
                            + to_string(colA)
                            + ", colB="
                            + to_string(colB)
                            + ", cols="
                            + to_string(cols)
                            + "]");
    }

    MultipleColumnStats retVal(*this);
    const ColumnStat& statsColA = getColumnStats()[colA];
    const ColumnStat& statsColB = getColumnStats()[colB];
    ColumnStat& statsColANew = retVal.columnStats[colA];
    ColumnStat& statsColBNew = retVal.columnStats[colB];

    if (statsColA.getTotalRows() == 0) {
        return retVal;
    }

    //Special case of joining with the same column
    if (colA == colB) {
        const double totalRows = (statsColA.getTotalRows()
                                    * statsColA.getTotalRows())
                                   / (statsColANew.getMaxVal()
                                      - statsColANew.getMinVal()
                                      + 1);

        for (size_t i = 0; i < cols; ++i) {
            retVal.columnStats[i].setTotalRows(totalRows);
            retVal.columnStats[i].setTotalRows(totalRows);
        }

        return retVal;
    }

    if (statsColA.getMinVal() > statsColB.getMinVal()) {
        statsColBNew.setMinVal(statsColA.getMinVal());
    }
    else if (statsColB.getMinVal() > statsColA.getMinVal()) {
        statsColANew.setMinVal(statsColB.getMinVal());
    }
    if (statsColA.getMaxVal() > statsColB.getMaxVal()) {
        statsColBNew.setMaxVal(statsColA.getMaxVal());
    }
    else if (statsColB.getMaxVal() > statsColA.getMaxVal()) {
        statsColANew.setMaxVal(statsColB.getMaxVal());
    }
    //No need to set anything else, we can be certain they will have the correct value
    const double totalRows = statsColA.getTotalRows()
                               / (statsColANew.getMaxVal()
                                  - statsColANew.getMinVal()
                                  + 1);
    statsColANew.setTotalRows(totalRows);
    statsColBNew.setTotalRows(totalRows);
    //Running this with cols as the current column doesn't skip anything
    //(we need to apply the same to all, including the filtered ones)
    updateOtherRows(cols, *this, retVal.columnStats, statsColANew, statsColA);
    return retVal;
}
MultipleColumnStats MultipleColumnStats::join(size_t colThis,
                                                  const MultipleColumnStats& other,
                                                  size_t colOther) const {
    if (colThis >= cols || colOther >= other.cols) {
        throw runtime_error("MultipleColumnStats::filterRange index out of bounds [colThis="
                            + to_string(colThis)
                            + ", this.cols="
                            + to_string(cols)
                            + ", colOther="
                            + to_string(colOther)
                            + ", other.cols="
                            + to_string(other.cols)
                            + "]");
    }

    MultipleColumnStats retVal(*this, other);
    const ColumnStat& statsColThis = getColumnStats()[colThis];
    const ColumnStat& statsColOther = other.getColumnStats()[colOther];
    ColumnStat& statsColThisNew = retVal.columnStats[colThis];
    ColumnStat& statsColOtherNew = retVal.columnStats[cols + colOther];

    if (statsColThis.getTotalRows() == 0) {
        for (size_t i = cols; i < retVal.cols; ++i) {
            ColumnStat& toChange = retVal.columnStats[i];
            toChange.setTotalRows(0);
            toChange.setUniqueValues(0);
        }
    }
    else if (statsColOther.getTotalRows() == 0) {
        for (size_t i = 0; i < cols; ++i) {
            ColumnStat& toChange = retVal.columnStats[i];
            toChange.setTotalRows(0);
            toChange.setUniqueValues(0);
        }
    }

    if (statsColThis.getMinVal() > statsColOther.getMinVal()) {
        statsColOtherNew.setMinVal(statsColThis.getMinVal());
    }
    else if (statsColOther.getMinVal() > statsColThis.getMinVal()) {
        statsColThisNew.setMinVal(statsColOther.getMinVal());
    }
    if (statsColThis.getMaxVal() > statsColOther.getMaxVal()) {
        statsColOtherNew.setMaxVal(statsColThis.getMaxVal());
    }
    else if (statsColOther.getMaxVal() > statsColThis.getMaxVal()) {
        statsColThisNew.setMaxVal(statsColOther.getMaxVal());
    }
    const uint64_t range = statsColThisNew.getMaxVal()
                           - statsColThisNew.getMinVal()
                           + 1;
    const double totalRows = (statsColThis.getTotalRows()
                                * statsColOther.getTotalRows())
                               / range;
    const double uniqueRows = (statsColThis.getUniqueValues()
                                 * statsColOther.getUniqueValues())
                                / range;
    statsColThisNew.setTotalRows(totalRows);
    statsColThisNew.setUniqueValues(uniqueRows);
    statsColOtherNew.setTotalRows(totalRows);
    statsColOtherNew.setUniqueValues(uniqueRows);
    updateOtherRows(colThis,
                    *this,
                    retVal.columnStats,
                    statsColThisNew,
                    statsColThis);
    updateOtherRows(colOther,
                    other,
                    retVal.columnStats + cols,
                    statsColOtherNew,
                    statsColOther);

    return retVal;
}

size_t MultipleColumnStats::getCols() const {
    return cols;
}

const ColumnStat* MultipleColumnStats::getColumnStats() const {
    return columnStats;
}

ostream& operator<<(ostream& os, const MultipleColumnStats& toPrint) {
    os << "[MultipleColumnStats cols=" << toPrint.cols << ", columnStats=";
    if (toPrint.getColumnStats() == nullptr) {
        os << "null";
    }
    else {
        os << "[";
        for (size_t i = 0; i < toPrint.cols; ++i) {
            os << "\n\t" << toPrint.getColumnStats()[i];
        }
        os << "]";
    }
    os << "]";
    return os;
}
