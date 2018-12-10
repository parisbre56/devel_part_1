/*
 * TableColumn.cpp
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#include "TableColumn.h"

using namespace std;

TableColumn::TableColumn() :
        tableNum(0), tableCol(0) {

}

TableColumn::TableColumn(uint32_t tableNum, size_t tableCol) :
        tableNum(tableNum), tableCol(tableCol) {

}

TableColumn::~TableColumn() {
    //Do nothing
}

uint32_t TableColumn::getTableNum() const {
    return tableNum;
}
void TableColumn::setTableNum(uint32_t tableNum) {
    this->tableNum = tableNum;
}

size_t TableColumn::getTableCol() const {
    return tableCol;
}
void TableColumn::setTableCol(size_t tableCol) {
    this->tableCol = tableCol;
}

ostream& operator<<(ostream& os, const TableColumn& toPrint) {
    os << "[TableColumn tableNum="
       << toPrint.tableNum
       << ", tableCol="
       << toPrint.tableCol
       << "]";
    return os;
}
