/*
 * TableColumn.cpp
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#include "TableColumn.h"

using namespace std;

TableColumn::TableColumn(uint32_t tableNum, size_t tableCol) :
        tableNum(tableNum), tableCol(tableCol) {

}

TableColumn::~TableColumn() {
    //Do nothing
}

uint32_t TableColumn::getTableNum() const {
    return tableNum;
}
uint32_t TableColumn::getTableCol() const {
    return tableCol;
}

ostream& operator<<(ostream& os, const TableColumn& toPrint) {
    os << "[TableColumn tableNum="
       << toPrint.tableNum
       << ", tableCol="
       << toPrint.tableCol
       << "]";
    return os;
}
