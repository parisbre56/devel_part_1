/*
 * TableColumn.h
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#ifndef TABLECOLUMN_H_
#define TABLECOLUMN_H_
class TableColumn;

#include <iostream>

class TableColumn {
    uint32_t tableNum;
    size_t tableCol;
public:
    TableColumn();
    TableColumn(uint32_t tableNum, size_t tableCol);
    TableColumn(const TableColumn& toCopy);
    TableColumn(TableColumn&& toMove) = delete;
    TableColumn& operator=(const TableColumn& toCopy) = delete;
    TableColumn& operator=(TableColumn&& toMove) = delete;
    virtual ~TableColumn();

    uint32_t getTableNum() const;
    void setTableNum(uint32_t tableNum);
    size_t getTableCol() const;
    void setTableCol(size_t tableCol);

    friend std::ostream& operator<<(std::ostream& os,
                                    const TableColumn& toPrint);
};

#endif /* TABLECOLUMN_H_ */
