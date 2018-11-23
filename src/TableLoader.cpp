/*
 * TableLoader.cpp
 *
 *  Created on: 22 Νοε 2018
 *      Author: parisbre56
 */

#include "TableLoader.h"

using namespace std;

#include <cstring>

#include <sstream>
#include <stdexcept>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>
#include <unistd.h>

#include "ConsoleOutput.h"

TableLoader::TableLoader(uint32_t arraySize) :
        arraySize(arraySize), tables(0), tableArray(new const Table*[arraySize]) {

}

TableLoader::TableLoader(TableLoader&& toMove) :
        arraySize(toMove.arraySize),
        tables(toMove.tables),
        tableArray(toMove.tableArray) {
    if (toMove.tableArray == nullptr)
        throw runtime_error("Table was already moved");
    toMove.tableArray = nullptr;
}

TableLoader::~TableLoader() {
    if (tableArray != nullptr) {
        for (uint32_t i = 0; i < tables; ++i) {
            delete tableArray[i];
        }
        delete[] tableArray;
    }
}

const Table& TableLoader::loadTable(string filePath) {
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

    char* addr = static_cast<char*>(mmap(nullptr, length,
    PROT_READ,
                                         MAP_PRIVATE, fd, 0u));
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

    return this->addTable(rows, cols, col_row_table, false);
}

const Table& TableLoader::addTable(uint64_t rows,
                                   size_t cols,
                                   const uint64_t * col_row_table,
                                   bool ownsMemory) {
    if (this->tables >= this->arraySize) {
        throw runtime_error("Reached loader limit. Can't load more tables.");
    }
    this->tableArray[this->tables] = new Table(rows,
                                               cols,
                                               col_row_table,
                                               ownsMemory);
    return *(this->tableArray[this->tables++]);
}

const Table& TableLoader::getTable(uint32_t index) const {
    if (index >= this->tables) {
        throw runtime_error("TableLoader.getTable failed. IndexOutOfBounds [tables="
                            + to_string(this->tables)
                            + ", index="
                            + to_string(index)
                            + "]");
    }
    return *(this->tableArray[index]);
}
uint32_t TableLoader::getTables() const {
    return tables;
}
uint32_t TableLoader::getArraySize() const {
    return arraySize;
}

std::ostream& operator<<(std::ostream& os, const TableLoader& toPrint) {
    os << "[TableLoader arraySize="
       << toPrint.arraySize
       << ", tables="
       << toPrint.tables
       << ", tableArray=";
    if (toPrint.tableArray == nullptr)
        os << "null";
    else {
        os << "[";
        for (uint32_t i = 0; i < toPrint.tables; ++i) {
            os << "\n\t" << i << ": " << *(toPrint.tableArray[i]);
        }
        os << "]";
    }
    os << "]";
    return os;
}
