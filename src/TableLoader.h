/*
 * TableLoader.h
 *
 *  Created on: 22 Νοε 2018
 *      Author: parisbre56
 */

#ifndef TABLELOADER_H_
#define TABLELOADER_H_
class TableLoader;

#include "Table.h"

#include <string>

#include "MultipleColumnStats.h"

class TableLoader {
protected:
    const uint32_t arraySize;
    uint32_t tables;
    const Table ** tableArray;
    const MultipleColumnStats ** tableStats;
public:
    TableLoader() = delete;
    explicit TableLoader(uint32_t arraySize);
    TableLoader(const TableLoader& toCopy) = delete;
    TableLoader(TableLoader&& toMove);
    TableLoader& operator=(const TableLoader& toCopy) = delete;
    TableLoader& operator=(TableLoader&& toMove) = delete;

    virtual ~TableLoader();

    const Table& loadTable(std::string filePath);
    const Table& addTable(uint64_t rows,
                          size_t cols,
                          const uint64_t * col_row_table,
                          bool ownsMemory,
                          const std::string tempFile);
    const Table& getTable(uint32_t index) const;
    const MultipleColumnStats& getStats(uint32_t index) const;
    uint32_t getTables() const;
    uint32_t getArraySize() const;

    friend std::ostream& operator<<(std::ostream& os,
                                    const TableLoader& toPrint);
};

#endif /* TABLELOADER_H_ */
