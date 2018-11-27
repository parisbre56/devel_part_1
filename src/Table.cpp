/*
 * Table.cpp
 *
 *  Created on: 18 Νοε 2018
 *      Author: parisbre56
 */

#include "Table.h"

#include <cstring>

#include <sstream>
#include <stdexcept>

using namespace std;

Table::Table(uint64_t rows,
             size_t cols,
             const uint64_t * col_row_table,
             bool ownsMemory) :
        rows(rows),
        cols(cols),
        col_row_table(col_row_table),
        ownsMemory(ownsMemory) {

}

Table::Table(Table&& toMove) :
        Table(toMove.rows, toMove.cols, toMove.col_row_table, toMove.ownsMemory) {
    if (toMove.ownsMemory) {
        toMove.ownsMemory = false;
    }
}

Table::~Table() {
    if (ownsMemory) {
        delete[] col_row_table;
    }
}

uint64_t Table::getRows() const {
    return rows;
}

size_t Table::getCols() const {
    return cols;
}

uint64_t Table::getValue(uint64_t row, size_t col) const {
    if (row >= rows) {
        throw runtime_error("OutOfBounds [row="
                            + to_string(row)
                            + ", rows="
                            + to_string(rows)
                            + "]");
    }
    if (col >= cols) {
        throw runtime_error("OutOfBounds [col="
                            + to_string(col)
                            + ", cols="
                            + to_string(cols)
                            + "]");
    }
    return col_row_table[col * rows + row];
}

std::ostream& operator<<(std::ostream& os, const Table& toPrint) {
    os << "[Table rows="
       << toPrint.rows
       << ", cols="
       << toPrint.cols
       << ", ownsMemory="
       << toPrint.ownsMemory
       << ", col_row_table=[";
    for (uint64_t row = 0; row < toPrint.rows; ++row) {
        os << "\n\t[";
        for (size_t col = 0; col < toPrint.cols; ++col) {
            if (col != 0) {
                os << ", ";
            }
            os << toPrint.getValue(row, col);
        }
        os << "]";
    }
    os << "]]";
    return os;
}
