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

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>
#include <unistd.h>

#include "ConsoleOutput.h"

using namespace std;

Table loadTable(string filePath) {
    ConsoleOutput consoleOutput("loadTable");
    CO_IFDEBUG(consoleOutput, "Loading file " << filePath);

    //Open file
    int fd = open(filePath.c_str(), O_RDONLY);
    if (fd == -1) {
        throw runtime_error("Unable to open file '"
                            + filePath
                            + "': ("
                            + to_string(errno)
                            + ") "
                            + strerror(errno));
    }

    CO_IFDEBUG(consoleOutput, "Opened file with FD: " << fd);

    // Obtain file size
    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        throw runtime_error("Unable to stat file '"
                            + filePath
                            + "' with descriptor '"
                            + to_string(fd)
                            + "': ("
                            + to_string(errno)
                            + ") "
                            + strerror(errno));
    }

    auto length = sb.st_size;
    CO_IFDEBUG(consoleOutput, "File length: " << length);

    char* addr = static_cast<char*>(mmap(nullptr,
                                         length,
                                         PROT_READ,
                                         MAP_PRIVATE,
                                         fd,
                                         0u));
    if (addr == MAP_FAILED) {
        throw runtime_error("Unable to map file "
                            + filePath
                            + "' with length '"
                            + to_string(length)
                            + "': ("
                            + to_string(errno)
                            + ") "
                            + strerror(errno));
    }

    uint64_t rows = *reinterpret_cast<uint64_t*>(addr);
    CO_IFDEBUG(consoleOutput, "Table has " << rows << " rows");
    addr += sizeof(rows);
    size_t cols = *reinterpret_cast<size_t*>(addr);
    CO_IFDEBUG(consoleOutput, "Table has " << cols << " cols");
    addr += sizeof(cols);
    uint64_t* col_row_table = reinterpret_cast<uint64_t*>(addr);

    if (close(fd) == -1) {
        throw runtime_error("Unable to close file '"
                            + filePath
                            + "' with descriptor '"
                            + to_string(fd)
                            + "': ("
                            + to_string(errno)
                            + ") "
                            + strerror(errno));
    }
    CO_IFDEBUG(consoleOutput, "Finished loading table");

    Table retTable(rows, cols, col_row_table, false);
    return retTable;
}

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

