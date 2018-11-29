/*
 * Join.cpp
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#include "Join.h"

using namespace std;

Join::Join(const TableLoader& tableLoader, uint32_t arraySize) :
        tableLoader(tableLoader),
        arraySize(arraySize),
        tableNum(0),
        tables(new uint32_t[arraySize]),
        filterNum(0),
        filters(new Filter*[arraySize]),
        joinRelationNum(0),
        joinRelations(new JoinRelation*[arraySize]),
        sumColumnNum(0),
        sumColumns(new TableColumn*[arraySize]) {

}

Join::~Join() {
    if (tables != nullptr) {
        delete[] tables;
    }
    if (filters != nullptr) {
        for (uint32_t i = 0; i < filterNum; ++i) {
            delete filters[i];
        }
        delete[] filters;
    }
    if (joinRelations != nullptr) {
        for (uint32_t i = 0; i < joinRelationNum; ++i) {
            delete joinRelations[i];
        }
        delete[] joinRelations;
    }
    if (sumColumns != nullptr) {
        for (uint32_t i = 0; i < sumColumnNum; ++i) {
            delete sumColumns[i];
        }
        delete[] sumColumns;
    }
}

void Join::addTable(uint32_t table) {
    if (tables == nullptr) {
        throw runtime_error("Join no longer valid, can't add table");
    }
    if (tableNum >= arraySize) {
        throw runtime_error("Reached limit, can't add more tables");
    }
    if (table >= tableLoader.getTables()) {
        throw runtime_error("addTable: unknown table [table="
                            + to_string(table)
                            + ", tableLoader.tables="
                            + to_string(tableLoader.getTables())
                            + "]");
    }
    tables[tableNum++] = table;
}
void Join::addTableRelationship(uint32_t tableA,
                                size_t columnA,
                                uint32_t tableB,
                                size_t columnB) {
    if (joinRelations == nullptr) {
        throw runtime_error("Join no longer valid, can't add relationship");
    }
    if (joinRelationNum >= arraySize) {
        throw runtime_error("Reached limit, can't add more relations");
    }
    if (tableA >= tableLoader.getTables()) {
        throw runtime_error("addTableRelationship: unknown table [tableA="
                            + to_string(tableA)
                            + ", tableLoader.tables="
                            + to_string(tableLoader.getTables())
                            + "]");
    }
    if (tableB >= tableLoader.getTables()) {
        throw runtime_error("addTableRelationship: unknown table [tableB="
                            + to_string(tableB)
                            + ", tableLoader.tables="
                            + to_string(tableLoader.getTables())
                            + "]");
    }
    if (columnA >= tableLoader.getTable(tableA).getCols()) {
        throw runtime_error("addTableRelationship: unknown column [tableA="
                            + to_string(tableA)
                            + ", columnA="
                            + to_string(columnA)
                            + ", tableLoader.tables[tableA].cols="
                            + to_string(tableLoader.getTable(tableA).getCols())
                            + "]");
    }
    if (columnB >= tableLoader.getTables()) {
        throw runtime_error("addTableRelationship: unknown column [tableB="
                            + to_string(tableB)
                            + ", columnB="
                            + to_string(columnB)
                            + ", tableLoader.tables[tableB].cols="
                            + to_string(tableLoader.getTable(tableB).getCols())
                            + "]");
    }
    joinRelations[joinRelationNum++] = new JoinRelation(tableA,
                                                        columnA,
                                                        tableB,
                                                        columnB);
}
void Join::addTableFilter(uint32_t table,
                          size_t column,
                          uint64_t filterNumber,
                          char type) {
    if (filters == nullptr) {
        throw runtime_error("Join no longer valid, can't add filter");
    }
    if (filterNum >= arraySize) {
        throw runtime_error("Reached limit, can't add more filters");
    }
    if (table >= tableLoader.getTables()) {
        throw runtime_error("addTableRelationship: unknown table [table="
                            + to_string(table)
                            + ", tableLoader.tables="
                            + to_string(tableLoader.getTables())
                            + "]");
    }
    if (column >= tableLoader.getTable(table).getCols()) {
        throw runtime_error("addTableRelationship: unknown column [table="
                            + to_string(table)
                            + ", column="
                            + to_string(column)
                            + ", tableLoader.tables[table].cols="
                            + to_string(tableLoader.getTable(table).getCols())
                            + "]");
    }
    switch (type) {
    case '>':
        filters[filterNum++] = new FilterGreater(table, column, filterNumber);
        break;
    case '=':
        filters[filterNum++] = new FilterEquals(table, column, filterNumber);
        break;
    case '<':
        filters[filterNum++] = new FilterLesser(table, column, filterNumber);
        break;
    default:
        throw runtime_error("Unknown filter type '" + to_string(type) + "'");
        break;
    }
}
void Join::addSumColumn(uint32_t table, size_t column) {
    if (sumColumns == nullptr) {
        throw runtime_error("Join no longer valid, can't add sum column");
    }
    if (sumColumnNum >= arraySize) {
        throw runtime_error("Reached limit, can't add more filters");
    }
    if (table >= tableLoader.getTables()) {
        throw runtime_error("addTableRelationship: unknown table [table="
                            + to_string(table)
                            + ", tableLoader.tables="
                            + to_string(tableLoader.getTables())
                            + "]");
    }
    if (column >= tableLoader.getTable(table).getCols()) {
        throw runtime_error("addTableRelationship: unknown column [table="
                            + to_string(table)
                            + ", column="
                            + to_string(column)
                            + ", tableLoader.tables[table].cols="
                            + to_string(tableLoader.getTable(table).getCols())
                            + "]");
    }
    sumColumns[sumColumnNum++] = new TableColumn(table, column);
}

JoinSumResult Join::performJoin() {
    //TODO checks for corner cases
    JoinSumResult retVal(sumColumnNum);
    //TODO
    return retVal;
}

ostream& operator<<(ostream& os, const Join& toPrint) {
    os << "[Join arraySize="
       << toPrint.arraySize
       << ", tableNum="
       << toPrint.tableNum
       << ", tables=";
    if (toPrint.tables == nullptr) {
        os << "null";
    }
    else {
        os << "[";
        for (uint32_t i = 0; i < toPrint.tableNum; ++i) {
            if (i > 0) {
                os << ", ";
            }
            os << "\n\t" << i << " -> " << toPrint.tables[i];
        }
        os << "]";
    }
    os << ", filterNum=" << toPrint.filterNum << ", filters=";
    if (toPrint.filters == nullptr) {
        os << "null";
    }
    else {
        os << "[";
        for (uint32_t i = 0; i < toPrint.filterNum; ++i) {
            if (i > 0) {
                os << ", ";
            }
            os << "\n\t" << *(toPrint.filters[i]);
        }
        os << "]";
    }
    os << ", joinRelationNum=" << toPrint.joinRelationNum << ", joinRelations=";
    if (toPrint.joinRelations == nullptr) {

    }
    else {
        os << "[";
        for (uint32_t i = 0; i < toPrint.joinRelationNum; ++i) {
            if (i > 0) {
                os << ", ";
            }
            os << "\n\t" << *(toPrint.joinRelations[i]);
        }
        os << "]";
    }
    os << ", sumColumnNum=" << toPrint.sumColumnNum << ", sumColumns=";
    if (toPrint.sumColumns == nullptr) {
        os << "null";
    }
    else {
        os << "[";
        for (uint32_t i = 0; i < toPrint.sumColumnNum; ++i) {
            if (i > 0) {
                os << ", ";
            }
            os << "\n\t" << *(toPrint.sumColumns[i]);
        }
        os << "]";
    }
    os << "]";
    return os;
}
