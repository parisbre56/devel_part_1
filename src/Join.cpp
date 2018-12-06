/*
 * Join.cpp
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#include "Join.h"

#include <limits>

#include "ResultContainer.h"
#include "ConsoleOutput.h"
#include "Relation.h"
#include "HashTable.h"
#include "BucketAndChain.h"

using namespace std;

//TODO dynamically choose
#define HASH_BITS 3
#define SUB_BUCKETS 3
const uint32_t buckets = 1 << HASH_BITS; //2^n
const uint32_t hashMask = (1 << HASH_BITS) - 1;

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
    ConsoleOutput consoleOutput("performJoin");
    JoinSumResult retVal(sumColumnNum);
    //checks for corner cases
    if (tableNum == 0 || sumColumnNum == 0) {
        return retVal;
    }
    //Determines which result holds the rows for table. Initialized to nullptr when a table has not been joined.
    const ResultContainer* resultLocation[this->tableNum] {/* init nullptr */};
    //Determines which side of the result holds this table. True for key, false for value
    bool resultKey[this->tableNum];
    //Current results of joining, each relation is stored in its own resultContainer
    ResultContainer resultContainers[this->joinRelationNum];
    bool isRelationProcessed[this->joinRelationNum] { /* init to false */};
    while (true) {
        uint32_t smallestRelationIndex;
        const JoinRelation * const smallestRelation = findSmallestRelation(isRelationProcessed,
                                                                           resultLocation,
                                                                           smallestRelationIndex);
        if (smallestRelation == nullptr) {
            CO_IFDEBUG(consoleOutput, "No more relations to process");
            break;
        }
        isRelationProcessed[smallestRelationIndex] = true;

        Relation relL(loadRelation(smallestRelation->leftNum,
                                   smallestRelation->leftCol,
                                   resultLocation,
                                   resultKey));
        Relation relR(loadRelation(smallestRelation->leftNum,
                                   smallestRelation->leftCol,
                                   resultLocation,
                                   resultKey));

        //Empty table means no sum
        if (relL.getNumTuples() == 0 || relR.getNumTuples() == 0) {
            return retVal;
        }

        if (relL.getNumTuples() >= relR.getNumTuples()) {
            resultContainers[smallestRelationIndex] = radixHashJoin(relR, relL);
            resultLocation[smallestRelation->leftNum] = resultContainers
                                                        + smallestRelationIndex;
            resultLocation[smallestRelation->rightNum] = resultContainers
                                                         + smallestRelationIndex;
            resultKey[smallestRelation->leftNum] = false;
            resultKey[smallestRelation->rightNum] = true;
        }
        else {
            resultContainers[smallestRelationIndex] = radixHashJoin(relL, relR);
            resultLocation[smallestRelation->leftNum] = resultContainers
                                                        + smallestRelationIndex;
            resultLocation[smallestRelation->rightNum] = resultContainers
                                                         + smallestRelationIndex;
            resultKey[smallestRelation->leftNum] = true;
            resultKey[smallestRelation->rightNum] = false;
        }

        //Empty join means no sum
        if (resultContainers[smallestRelationIndex].getResultCount() == 0) {
            return retVal;
        }
    }
    //TODO checks for corner cases
    //TODO perform join for each relation
    //TODO keep array with metadata for where the filtered rows of the table are kept
    //TODO  start with smallest relations
    //TODO  join for each relation
    //TODO  break if empty at any time
    //TODO cartesian product for all tables without resultLocation
    //TODO and cartesian product for all unconnected joins
    //TODO
    return retVal;
}

/** relR is stored in key, relS is stored in value **/
ResultContainer Join::radixHashJoin(Relation& relR, Relation& relS) const {
    ConsoleOutput consoleOutput("RadixHashJoin");
    consoleOutput.errorOutput() << "JOIN EXECUTION STARTED" << endl;

    HashTable rHash(relR, buckets, &Join::hashFunc);
    HashTable sHash(relS, buckets, &Join::hashFunc);
    CO_IFDEBUG(consoleOutput, "Hashes generated");
    CO_IFDEBUG(consoleOutput, "rHash=" << rHash);
    CO_IFDEBUG(consoleOutput, "sHash=" << sHash);

    ResultContainer retResult;
    for (uint32_t i = 0; i < buckets; ++i) {
        CO_IFDEBUG(consoleOutput, "Processing bucket " << i);

        if (rHash.getTuplesInBucket(i) == 0
            || sHash.getTuplesInBucket(i) == 0) {
            CO_IFDEBUG(consoleOutput,
                       "Skipping bucket " << i << ": 0 rows [R=" << rHash.getTuplesInBucket(i) << ", S=" << sHash.getTuplesInBucket(i) << "]");
            continue;
        }

        BucketAndChain rChain(rHash, i, SUB_BUCKETS, &Join::hashFuncChain);
        CO_IFDEBUG(consoleOutput, "Created subHashTable " << rChain);
        rChain.join(sHash, i, retResult);
    }

    consoleOutput.errorOutput() << "JOIN EXECUTION ENDED" << endl;
    return retResult;
}

uint32_t Join::hashFunc(uint32_t buckets, uint64_t toHash) const {
    //We ignore buckets, we don't really need it
    return hashMask & toHash;
}

uint32_t Join::hashFuncChain(uint32_t buckets, uint64_t toHash) const {
    return toHash % buckets;
}

JoinRelation* Join::findSmallestRelation(const bool* const isRelationProcessed,
                                         const ResultContainer* const * const resultLocation,
                                         uint32_t& smallestRelationIndex) const {
    JoinRelation* smallestRelation = nullptr;
    uint64_t smallestRelationValue = numeric_limits<uint64_t>::max();
    for (uint32_t i = 0; i < this->joinRelationNum; ++i) {
        //Skip already processed relations
        if (isRelationProcessed[i]) {
            continue;
        }

        const JoinRelation * const currRel = joinRelations[i];

        //Check left part of the relation
        {
            const uint32_t relLeftJoinTable = currRel->leftNum;
            const ResultContainer* leftResultContainerLoaded = resultLocation[relLeftJoinTable];
            const uint64_t relLeftRows;
            if (leftResultContainerLoaded == nullptr) {
                relLeftRows = tableLoader.getTable(tables[relLeftJoinTable]).getRows();
            }
            else {
                relLeftRows = leftResultContainerLoaded->getResultCount();
            }
            if (relLeftRows < smallestRelationValue) {
                smallestRelationValue = relLeftRows;
                smallestRelation = currRel;
                smallestRelationIndex = i;
            }
        }

        //Check right part of the relation
        {
            const uint32_t relRightJoinTable = currRel->rightNum;
            const ResultContainer* rightResultContainerLoaded = resultLocation[relRightJoinTable];
            const uint64_t relRightRows;
            if (rightResultContainerLoaded == nullptr) {
                relRightRows = tableLoader.getTable(tables[relRightJoinTable]).getRows();
            }
            else {
                relRightRows = rightResultContainerLoaded->getResultCount();
            }
            if (relRightRows < smallestRelationValue) {
                smallestRelationValue = relRightRows;
                smallestRelation = currRel;
                smallestRelationIndex = i;
            }
        }
    }
    return smallestRelation;
}

Relation Join::loadRelation(const uint32_t tableReference,
                            const size_t tableCol,
                            const ResultContainer* const * const resultLocation,
                            const bool* const resultKey) const {
    const ResultContainer* resultContainerLoaded = resultLocation[tableReference];
    const Table& joinTableLoaded = tableLoader.getTable(tables[tableReference]);
    if (resultContainerLoaded == nullptr) {
        const uint64_t rows = joinTableLoaded.getRows();
        Relation retVal(rows);
        const uint64_t * currRow = joinTableLoaded.getCol(tableCol);
        for (uint64_t i = 0; i < rows; i++, currRow++) {
            retVal.addTuple(i, *currRow);
        }
        //TODO handle filters for this table
        return retVal;
    }
    //Else, if resultContainerLoaded is not null (i.e. table was part of a previously joined relation)
    Relation retVal(resultContainerLoaded->getResultCount());
    resultContainerLoaded->loadToRelation(retVal,
                                          joinTableLoaded.getCol(tableCol),
                                          resultKey[tableReference]);
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
