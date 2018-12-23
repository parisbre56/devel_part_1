/*
 * ColumnStats.cpp
 *
 *  Created on: 17 Δεκ 2018
 *      Author: parisbre56
 */

#include "MultipleColumnStats.h"

#include <cstring>
#include <cmath>

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
        for (uint64_t i = 0; i < rows; ++i) {
            uint64_t encounteredIndex = currCol[i] - minVal;
            if (encounteredIndex >= maxLength) {
                currStat.incrementUniqueValues();
            }
            else if (!(encountered[encounteredIndex])) {
                encountered[encounteredIndex] = true;
                currStat.incrementUniqueValues();
            }
        }
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

MultipleColumnStats::MultipleColumnStats(MultipleColumnStats&& toMove) :
        cols(toMove.cols), columnStats(toMove.columnStats) {
    if (toMove.columnStats == nullptr) {
        throw runtime_error("stats have already been moved");
    }
    toMove.columnStats = nullptr;
}

MultipleColumnStats::~MultipleColumnStats() {
    if (columnStats != nullptr) {
        delete[] columnStats;
    }
}

void MultipleColumnStats::updateOtherRows(size_t col,
                     const MultipleColumnStats& current,
                                          MultipleColumnStats& retVal,
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
        ColumnStat& currColStatNew = retVal.columnStats[i];
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
    updateOtherRows(col, *this, retVal, colStatNew, colStat);
    return retVal;
}
MultipleColumnStats MultipleColumnStats::filterRange(size_t col,
                                                     uint64_t greaterOrEqualTo,
                                                     uint64_t lessThanOrEqualTo) const {
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
    updateOtherRows(col, *this, retVal, colStatNew, colStat);
    return retVal;
}
MultipleColumnStatsPair MultipleColumnStats::joinSame(size_t colA,
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

    MultipleColumnStatsPair retVal(*this, *this);
    const ColumnStat& statsColA = getColumnStats()[colA];
    const ColumnStat& statsColB = getColumnStats()[colB];
    ColumnStat& statsColANewLeft = retVal.getLeft().columnStats[colA];
    ColumnStat& statsColANewRight = retVal.getRight().columnStats[colA];
    ColumnStat& statsColBNewLeft = retVal.getLeft().columnStats[colB];
    ColumnStat& statsColBNewRight = retVal.getRight().columnStats[colB];

    //Special case of joining with the same column
    if (colA == colB) {
        const uint64_t totalRows = (statsColA.getTotalRows()
                                    * statsColA.getTotalRows())
                                   / (statsColANewLeft.getMaxVal()
                                      - statsColANewLeft.getMinVal()
                                      + 1);

        for (size_t i = 0; i < cols; ++i) {
            retVal.getLeft().columnStats[i].setTotalRows(totalRows);
            retVal.getRight().columnStats[i].setTotalRows(totalRows);
        }

        return retVal;
    }

    if (statsColA.getMinVal() > statsColB.getMinVal()) {
        statsColBNewLeft.setMinVal(statsColA.getMinVal());
        statsColBNewRight.setMinVal(statsColA.getMinVal());
    }
    else if (statsColB.getMinVal() > statsColA.getMinVal()) {
        statsColANewLeft.setMinVal(statsColB.getMinVal());
        statsColANewRight.setMinVal(statsColB.getMinVal());
    }
    if (statsColA.getMaxVal() > statsColB.getMaxVal()) {
        statsColBNewLeft.setMaxVal(statsColA.getMaxVal());
        statsColBNewRight.setMaxVal(statsColA.getMaxVal());
    }
    else if (statsColB.getMaxVal() > statsColA.getMaxVal()) {
        statsColANewLeft.setMaxVal(statsColB.getMaxVal());
        statsColANewRight.setMaxVal(statsColB.getMaxVal());
    }
    //No need to set anything else, we can be certain they will have the correct value
    const uint64_t totalRows = statsColA.getTotalRows()
                               / (statsColANewLeft.getMaxVal()
                                  - statsColANewLeft.getMinVal()
                                  + 1);
    statsColANewLeft.setTotalRows(totalRows);
    statsColANewRight.setTotalRows(totalRows);
    statsColBNewLeft.setTotalRows(totalRows);
    statsColBNewRight.setTotalRows(totalRows);
    //Running this with cols as the current column doesn't skip anything
    updateOtherRows(cols, *this, retVal.getLeft(), statsColANewLeft, statsColA);
    updateOtherRows(cols,
                    *this,
                    retVal.getRight(),
                    statsColANewRight,
                    statsColA);
    return retVal;
}
MultipleColumnStatsPair MultipleColumnStats::join(size_t colThis,
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

    MultipleColumnStatsPair retVal(*this, other);
    const ColumnStat& statsColThis = getColumnStats()[colThis];
    const ColumnStat& statsColOther = other.getColumnStats()[colOther];
    ColumnStat& statsColThisNew = retVal.getLeft().columnStats[colThis];
    ColumnStat& statsColOtherNew = retVal.getRight().columnStats[colOther];

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
    const uint64_t totalRows = (statsColThis.getTotalRows()
                          * statsColOther.getTotalRows())
                         / range;
    const uint64_t uniqueRows = (statsColThis.getUniqueValues()
                           * statsColOther.getUniqueValues())
                          / range;
    statsColThisNew.setTotalRows(totalRows);
    statsColThisNew.setUniqueValues(uniqueRows);
    statsColOtherNew.setTotalRows(totalRows);
    statsColOtherNew.setUniqueValues(uniqueRows);
    updateOtherRows(colThis,
                    *this,
                    retVal.getLeft(),
                    statsColThisNew,
                    statsColThis);
    updateOtherRows(colOther,
                    other,
                    retVal.getRight(),
                    statsColOtherNew,
                    statsColOther);

    return retVal;
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
