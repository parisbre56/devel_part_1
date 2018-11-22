/*
 * Table.h
 *
 *  Created on: 18 Νοε 2018
 *      Author: parisbre56
 */

#ifndef TABLE_H_
#define TABLE_H_

#include <cstdint>
#include <string>

class Table {
protected:
    const uint64_t rows;
    const size_t cols;
    /** A 2d table stored serially (so we have the entire first col, then the entire second col, etc.)
     * Total number of entries is rows*cols.
     * To find the i-th row of the j-th col you need to access [j*rows + i] with i,j zero based **/
    const uint64_t * const col_row_table;
    /** True if the col_row_table should be deleted, otherwise false **/
    bool ownsMemory;

public:
    Table() = delete;
    Table(const Table& toCopy) = delete;
    Table(Table&& toMove);
    Table& operator=(const Table& toCopy) = delete;
    Table& operator=(Table&& toMove) = delete;
    /** Load a table from the given binary file **/
    Table(uint64_t rows,
          size_t cols,
          const uint64_t * col_row_table,
          bool ownsMemory);
    virtual ~Table();

    uint64_t getValue(uint64_t row, size_t col) const;

    friend std::ostream& operator<<(std::ostream& os, const Table& toPrint);
};

Table loadTable(std::string filePath);

#endif /* TABLE_H_ */
