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
#include "FilterSameTable.h"
#include "HashFunctionBitmask.h"
#include "HashFunctionModulo.h"

using namespace std;

Join::Join(const TableLoader& tableLoader, uint32_t arraySize) :
        tableLoader(tableLoader),
        arraySize(arraySize),
        tableNum(0),
        tables(new uint32_t[arraySize]),
        filterNum(0),
        filters(new const Filter*[arraySize]),
        joinRelationNum(0),
        joinRelations(new const JoinRelation*[arraySize]),
        sumColumnNum(0),
        sumColumns(new const TableColumn*[arraySize]),
        resultContainers(nullptr),
        oldOrder(nullptr),
        newOrder(nullptr) {

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
    if (resultContainers != nullptr) {
        for (uint32_t i = 0; i < tableNum; ++i) {
            if (resultContainers[i] != nullptr) {
                const ResultContainer* toDelete = resultContainers[i];
                for (uint32_t j = i + 1; j < tableNum; ++j) {
                    if (resultContainers[j] == toDelete) {
                        resultContainers[j] = nullptr;
                    }
                }
                delete toDelete;
            }
        }
        delete[] resultContainers;
    }
    if (oldOrder != nullptr) {
        delete oldOrder;
    }
    if (newOrder != nullptr) {
        delete newOrder;
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
    if (tableA >= tableNum) {
        throw runtime_error("addTableRelationship: unknown table [tableA="
                            + to_string(tableA)
                            + ", tableNum="
                            + to_string(tableNum)
                            + "]");
    }
    if (tableB >= tableNum) {
        throw runtime_error("addTableRelationship: unknown table [tableB="
                            + to_string(tableB)
                            + ", tableNum="
                            + to_string(tableNum)
                            + "]");
    }
    if (columnA >= tableLoader.getTable(tables[tableA]).getCols()) {
        throw runtime_error("addTableRelationship: unknown column [tableA="
                            + to_string(tableA)
                            + ", loadedTableA="
                            + to_string(tables[tableA])
                            + ", columnA="
                            + to_string(columnA)
                            + ", tableLoader.getTable(tables[tableA]).getCols()="
                            + to_string(tableLoader.getTable(tables[tableA]).getCols())
                            + "]");
    }
    if (columnB >= tableLoader.getTable(tables[tableB]).getCols()) {
        throw runtime_error("addTableRelationship: unknown column [tableB="
                            + to_string(tableB)
                            + ", loadedTableB="
                            + to_string(tables[tableB])
                            + ", columnB="
                            + to_string(columnB)
                            + ", tableLoader.getTable(tables[tableB]).getCols()="
                            + to_string(tableLoader.getTable(tables[tableB]).getCols())
                            + "]");
    }
    if (tableA == tableB) {
        if (filters == nullptr) {
            throw runtime_error("Join no longer valid, can't add same table filter");
        }
        if (filterNum >= arraySize) {
            throw runtime_error("Reached limit, can't add same table filter");
        }
        filters[filterNum++] = new FilterSameTable(tableA, columnA, columnB);
    }
    else {
        if (joinRelationNum >= arraySize) {
            throw runtime_error("Reached limit, can't add more relations");
        }
        joinRelations[joinRelationNum++] = new JoinRelation(tableA,
                                                            columnA,
                                                            tableB,
                                                            columnB);
    }
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
    if (table >= tableNum) {
        throw runtime_error("addTableRelationship: unknown table [table="
                            + to_string(table)
                            + ", tableNum="
                            + to_string(tableNum)
                            + "]");
    }
    if (column >= tableLoader.getTable(tables[table]).getCols()) {
        throw runtime_error("addTableRelationship: unknown column [table="
                            + to_string(table)
                            + ", loadedTable="
                            + to_string(tables[table])
                            + ", column="
                            + to_string(column)
                            + ", tableLoader.getTable(tables[table]).getCols()="
                            + to_string(tableLoader.getTable(tables[table]).getCols())
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
    if (table >= tableNum) {
        throw runtime_error("addTableRelationship: unknown table [table="
                            + to_string(table)
                            + ", tableNum="
                            + to_string(tableNum)
                            + "]");
    }
    if (column >= tableLoader.getTable(tables[table]).getCols()) {
        throw runtime_error("addTableRelationship: unknown column [table="
                            + to_string(table)
                            + ", loadedTable="
                            + to_string(tables[table])
                            + ", column="
                            + to_string(column)
                            + ", tableLoader.getTable(tables[table]).getCols()="
                            + to_string(tableLoader.getTable(tables[table]).getCols())
                            + "]");
    }
    sumColumns[sumColumnNum++] = new TableColumn(table, column);
}

void Join::storeResut(ResultContainer* newResult) {
    //Delete old results and store new results
    const bool* resultUsedRows = newResult->getUsedRows();
    for (uint32_t i = 0; i < tableNum; ++i) {
        if (resultUsedRows[i] && resultContainers[i] != newResult) {
            if (resultContainers[i] != nullptr) {
                const ResultContainer* toDelete = resultContainers[i];
                for (uint32_t j = i + 1; j < tableNum; ++j) {
                    if (resultContainers[j] == toDelete) {
                        resultContainers[j] = newResult;
                    }
                }
                delete toDelete;
            }
            resultContainers[i] = newResult;
        }
    }
}

JoinSumResult Join::performJoin() {
    ConsoleOutput consoleOutput("performJoin");
    JoinSumResult retVal(sumColumnNum);
    //checks for corner cases
    if (tableNum == 0 || sumColumnNum == 0) {
        CO_IFDEBUG(consoleOutput,
                   "No data, not performing join [tableNum=" << tableNum << ", sumColumnNum=" << sumColumnNum << "]");
        return retVal;
    }
    //Special case: only one table
    if (tableNum == 1) {
        CO_IFDEBUG(consoleOutput, "Processing single table");
        Relation rel(loadRelation(0, 0, nullptr));
        if (rel.getNumTuples() == 0) {
            CO_IFDEBUG(consoleOutput, "Filtered table empty, no result");
            return retVal;
        }
        retVal.setHasResults();

        const uint64_t * sumCols[sumColumnNum];
        uint32_t sumTable[sumColumnNum];
        fillSumTables(sumCols, sumTable);

        fillSumsFromRelation(retVal, rel, sumCols, sumTable);

        return retVal;
    }
    //Determines which result holds the rows for table. Initialized to nullptr when a table has not been joined.
    bool isRelationProcessed[joinRelationNum] { /* init to false */};
    resultContainers = new ResultContainer*[tableNum] {/* init to nullptr */};

    //Apply filters to all table stats

    newOrder = new JoinOrderContainer(tableNum);
    for (uint32_t i = 0; i < tableNum; ++i) {
        JoinOrder toAdd(tableNum, i);
        newOrder->addIfBetterMove(move(toAdd), loadStats(i));
    }
    for (uint32_t permutationSize = 1; permutationSize < tableNum;
            ++permutationSize) {
        oldOrder = newOrder;
        newOrder = new JoinOrderContainer(oldOrder->getUsed() * 2);
        for (uint32_t subsetIndex; subsetIndex < oldOrder->getUsed();
                ++subsetIndex) {
            const JoinOrder& currentSubset = *((oldOrder->getJoinOrders())[subsetIndex]);
            const MultipleColumnStats& currentStat = *((oldOrder->getStats())[subsetIndex]);
            if (currentSubset.getOrderedTables() != permutationSize) {
                break;
            }
        }
    }

    //Now that we've found the join order we no longer need these things, so free up some space

    while (true) {
        uint32_t smallestRelationIndex;
        uint32_t sameTableRelations;
        const JoinRelation * const smallestRelation = findSmallestRelation(isRelationProcessed,
                                                                           smallestRelationIndex,
                                                                           sameTableRelations);
        if (smallestRelation == nullptr) {
            CO_IFDEBUG(consoleOutput, "No more relations to process");
            break;
        }
        CO_IFDEBUG(consoleOutput, "Smallest relation " << *smallestRelation);

        TableColumn colsToProcessLeft[sameTableRelations];
        TableColumn colsToProcessRight[sameTableRelations];
        sameTableRelations = 0;
        for (uint32_t relationIndex = 0; relationIndex < joinRelationNum;
                ++relationIndex) {
            const JoinRelation& currRelation = *(joinRelations[relationIndex]);
            unsigned char cmp = currRelation.sameJoinAs(*smallestRelation,
                                                        resultContainers);
            if (cmp) {
                CO_IFDEBUG(consoleOutput,
                           "Will process relation [cmp=" << to_string(cmp) << ", currRelation=" << currRelation << "]");
                isRelationProcessed[relationIndex] = true;
                if (cmp == 1) {
                    colsToProcessLeft[sameTableRelations].setTableNum(currRelation.getLeftNum());
                    colsToProcessLeft[sameTableRelations].setTableCol(currRelation.getLeftCol());
                    colsToProcessRight[sameTableRelations].setTableNum(currRelation.getRightNum());
                    colsToProcessRight[sameTableRelations].setTableCol(currRelation.getRightCol());
                }
                else if (cmp == 2) {
                    colsToProcessLeft[sameTableRelations].setTableNum(currRelation.getRightNum());
                    colsToProcessLeft[sameTableRelations].setTableCol(currRelation.getRightCol());
                    colsToProcessRight[sameTableRelations].setTableNum(currRelation.getLeftNum());
                    colsToProcessRight[sameTableRelations].setTableCol(currRelation.getLeftCol());
                }
                else {
                    throw runtime_error("Unknown cmp while searching for matching relations");
                }
                sameTableRelations++;
            }
        }

        Relation relL(loadRelation(smallestRelation->getLeftNum(),
                                   sameTableRelations,
                                   colsToProcessLeft));
        Relation relR(loadRelation(smallestRelation->getRightNum(),
                                   sameTableRelations,
                                   colsToProcessRight));

        //Empty table means no sum
        if (relL.getNumTuples() == 0 || relR.getNumTuples() == 0) {
            CO_IFDEBUG(consoleOutput,
                       "Relation empty, not performing join [relL.numTuples=" << relL.getNumTuples() << ", relR.numTuples=" << relR.getNumTuples() << "]");
            return retVal;
        }

        ResultContainer* newResult = new ResultContainer(
                (relL.getNumTuples() >= relR.getNumTuples()) ? radixHashJoin(relR,
                                                                             relL) :
                                                               radixHashJoin(relL,
                                                                             relR));
        CO_IFDEBUG(consoleOutput, "Join result: "<< *newResult);

        //Delete old results and store new results
        storeResut(newResult);

        //Empty join means no sum
        if (newResult->getResultCount() == 0) {
            CO_IFDEBUG(consoleOutput, "Result empty, not performing join");
            return retVal;
        }
    }
    CO_IFDEBUG(consoleOutput,
               "Finished processing relations, processing cartesian products");
    while (true) {
        if (resultContainers[0] != nullptr) {
            bool allJoined = true;
            for (uint32_t i = 0; i < tableNum; ++i) {
                if (!resultContainers[0]->getUsedRows()[i]) {
                    allJoined = false;
                    break;
                }
            }
            if (allJoined) {
                break;
            }
        }

        uint64_t smallestResultNum = numeric_limits<uint64_t>::max();
        uint32_t smallestResultIndex = tableNum;
        ResultContainer* smallestResult = nullptr;
        for (uint32_t i = 0; i < tableNum; ++i) {
            if (resultContainers[i] != nullptr) {
                if (resultContainers[i]->getResultCount() < smallestResultNum) {
                    smallestResultIndex = i;
                    smallestResultNum = resultContainers[i]->getResultCount();
                    smallestResult = resultContainers[i];
                }
            }
            else {
                const Table& currTable = tableLoader.getTable(tables[i]);
                if (currTable.getRows() < smallestResultNum) {
                    smallestResultIndex = i;
                    smallestResultNum = tableLoader.getTable(tables[i]).getRows();
                    smallestResult = nullptr;
                }
            }
        }

        uint64_t secondSmallestResultNum = numeric_limits<uint64_t>::max();
        uint32_t secondSmallestResultIndex = tableNum;
        ResultContainer* secondSmallestResult = nullptr;
        for (uint32_t i = 0; i < tableNum; ++i) {
            //Skip the first smallest result
            if (smallestResultIndex == i
                || (smallestResult != nullptr
                    && resultContainers[i] == smallestResult)) {
                continue;
            }
            if (resultContainers[i] != nullptr) {
                if (resultContainers[i]->getResultCount()
                    < secondSmallestResultNum) {
                    secondSmallestResultIndex = i;
                    secondSmallestResultNum = resultContainers[i]->getResultCount();
                    secondSmallestResult = resultContainers[i];
                }
            }
            else {
                const Table& currTable = tableLoader.getTable(tables[i]);
                if (currTable.getRows() < secondSmallestResultNum) {
                    secondSmallestResultIndex = i;
                    secondSmallestResultNum = tableLoader.getTable(tables[i]).getRows();
                    secondSmallestResult = nullptr;
                }
            }
        }

        CO_IFDEBUG(consoleOutput,
                   "Cartesian product [smallestResultIndex=" << smallestResultIndex << ", smallestResult=" << (void*)smallestResult << ", secondSmallestResultIndex=" << secondSmallestResult << ", secondSmallestResult=" << (void*)secondSmallestResult << "]");
        Relation relR(loadRelation(smallestResultIndex, 0, nullptr));
        Relation relS(loadRelation(secondSmallestResultIndex, 0, nullptr));

        ResultContainer* newResult = new ResultContainer(relR * relS);
        //Delete old results and store new results
        storeResut(newResult);

        //Empty join means no sum
        if (newResult->getResultCount() == 0) {
            CO_IFDEBUG(consoleOutput, "Result empty, not performing join");
            return retVal;
        }
    }

    fillSums(retVal);

    return retVal;
}

void Join::fillSumTables(const uint64_t* * const sumCols,
                         uint32_t * const sumTable) const {
    for (uint32_t i = 0; i < sumColumnNum; ++i) {
        const TableColumn& currSumColumn = *(sumColumns[i]);
        sumCols[i] = tableLoader.getTable(tables[sumTable[i] = currSumColumn.getTableNum()]).getCol(currSumColumn.getTableCol());
    }
}

void Join::fillSums(JoinSumResult& retVal) const {
    if (resultContainers[0]->getResultCount() == 0) {
        return;
    }
    retVal.setHasResults();

    const uint64_t * sumCols[sumColumnNum];
    uint32_t sumTable[sumColumnNum];
    fillSumTables(sumCols, sumTable);

    const Result* currResult = resultContainers[0]->getFirstResultBlock();
    while (currResult != nullptr) {
        fillSumsFromRelation(retVal,
                             currResult->getRelation(),
                             sumCols,
                             sumTable);
        currResult = currResult->getNext();
    }
}

void Join::fillSumsFromRelation(JoinSumResult& retVal,
                                const Relation& currRel,
                                const uint64_t * const * const sumCols,
                                const uint32_t * const sumTable) const {
    const uint64_t relRows = currRel.getNumTuples();
    const Tuple * const * tuples = currRel.getTuples();
    for (uint64_t rowNum = 0; rowNum < relRows; ++rowNum) {
        const Tuple& currTuple = *(tuples[rowNum]);
        for (uint32_t i = 0; i < sumColumnNum; ++i) {
            retVal.addSum(i, sumCols[i][currTuple.getTableRow(sumTable[i])]);
        }
    }
}

unsigned char Join::getBitmaskSize(const uint64_t rows) const {
    if (rows < 100) {
        return 1;
    }
    if (rows < 1000) {
        return 2;
    }
    if (rows < 10000) {
        return 3;
    }
    if (rows < 100000) {
        return 4;
    }
    if (rows < 1000000) {
        return 5;
    }
    if (rows < 3000000) {
        return 6;
    }
    if (rows < 5000000) {
        return 7;
    }
    if (rows < 7000000) {
        return 8;
    }
    if (rows < 10000000) {
        return 9;
    }
    if (rows < 40000000) {
        return 10;
    }
    if (rows < 70000000) {
        return 11;
    }
    if (rows < 100000000) {
        return 12;
    }
    return 13;
}

uint32_t Join::getBucketAndChainBuckets(const uint64_t tuplesInBucket) const {
    return tuplesInBucket / 10;
}

MultipleColumnStats Join::loadStats(const uint32_t table) const {
    MultipleColumnStats retVal(tableLoader.getStats(tables[table]));
    for (uint32_t i = 0; i < filterNum; ++i) {
        const Filter& currFilter = *(filters[i]);
        if (currFilter.getTable() == table) {
            retVal = currFilter.applyFilter(retVal);
        }
    }
    return retVal;
}

/** relR is stored in key, relS is stored in value **/
ResultContainer Join::radixHashJoin(const Relation& relR,
                                    const Relation& relS) const {
    ConsoleOutput consoleOutput("RadixHashJoin");
    //consoleOutput.errorOutput() << "JOIN EXECUTION STARTED" << endl;

    HashFunctionBitmask bitmask(getBitmaskSize(relR.getNumTuples()));

    HashTable rHash(relR, bitmask);
    HashTable sHash(relS, bitmask);
    CO_IFDEBUG(consoleOutput, "Hashes generated");
    CO_IFDEBUG(consoleOutput, "rHash=" << rHash);
    CO_IFDEBUG(consoleOutput, "sHash=" << sHash);

    ResultContainer retResult(relR.getNumTuples() * relS.getNumTuples(),
                              relR.getSizeTableRows(),
                              0);
    for (uint32_t i = 0; i < tableNum; ++i) {
        if (relR.getUsedRow(i) || relS.getUsedRow(i)) {
            retResult.setUsedRow(i);
        }
    }
    uint32_t buckets = rHash.getBuckets();
    for (uint32_t i = 0; i < buckets; ++i) {
        CO_IFDEBUG(consoleOutput, "Processing bucket " << i);

        if (rHash.getTuplesInBucket(i) == 0
            || sHash.getTuplesInBucket(i) == 0) {
            CO_IFDEBUG(consoleOutput,
                       "Skipping bucket " << i << ": 0 rows [R=" << rHash.getTuplesInBucket(i) << ", S=" << sHash.getTuplesInBucket(i) << "]");
            continue;
        }

        HashFunctionModulo modulo(getBucketAndChainBuckets(rHash.getTuplesInBucket(i)));
        BucketAndChain rChain(rHash, i, modulo);
        CO_IFDEBUG(consoleOutput, "Created subHashTable " << rChain);
        rChain.join(sHash, i, retResult);
    }

    //consoleOutput.errorOutput() << "JOIN EXECUTION ENDED" << endl;
    return retResult;
}

const JoinRelation* Join::findSmallestRelation(const bool* const isRelationProcessed,
                                               uint32_t& smallestRelationIndex,
                                               uint32_t& sameTableRelations) const {
    const JoinRelation* smallestRelation = nullptr;
    uint64_t smallestRelationValue = numeric_limits<uint64_t>::max();
    sameTableRelations = 0;
    for (uint32_t i = 0; i < joinRelationNum; ++i) {
        //Skip already processed relations
        if (isRelationProcessed[i]) {
            continue;
        }

        const JoinRelation * const currRel = joinRelations[i];

        //Check left part of the relation

        const uint32_t relLeftJoinTable = currRel->getLeftNum();
        const ResultContainer* leftResultContainerLoaded = resultContainers[relLeftJoinTable];
        const uint64_t relLeftRows =
                (leftResultContainerLoaded == nullptr) ? (tableLoader.getTable(tables[relLeftJoinTable]).getRows()) :
                                                         (leftResultContainerLoaded->getResultCount());
        if (relLeftRows < smallestRelationValue) {
            smallestRelationValue = relLeftRows;
            smallestRelation = currRel;
            smallestRelationIndex = i;
            sameTableRelations = 0;
        }

        //Check right part of the relation
        const uint32_t relRightJoinTable = currRel->getRightNum();
        const ResultContainer* rightResultContainerLoaded = resultContainers[relRightJoinTable];
        const uint64_t relRightRows =
                (rightResultContainerLoaded == nullptr) ? (tableLoader.getTable(tables[relRightJoinTable]).getRows()) :
                                                          (rightResultContainerLoaded->getResultCount());

        if (relRightRows < smallestRelationValue) {
            smallestRelationValue = relRightRows;
            smallestRelation = currRel;
            smallestRelationIndex = i;
            sameTableRelations = 0;
        }

        if (smallestRelation->sameJoinAs(*currRel, resultContainers)) {
            ++sameTableRelations;
        }
    }
    return smallestRelation;
}

Relation Join::loadRelation(const uint32_t tableReference,
                            const uint32_t colsToProcessNum,
                            const TableColumn* const colsToProcess) const {
    ConsoleOutput consoleOutput("Join::loadRelation");
    CO_IFDEBUG(consoleOutput,
               "Join::loadRelation [tableReference="<<tableReference<<", colsToProcessNum="<<colsToProcessNum<<", colsToProcess=[");
    if (colsToProcess == nullptr) {
        CO_IFDEBUG(consoleOutput, "null");
    }
    else {
        for (uint32_t i = 0; i < colsToProcessNum; ++i) {
            CO_IFDEBUG(consoleOutput, "\t" << colsToProcess[i]);
        }
    }
    CO_IFDEBUG(consoleOutput, "]");
    const ResultContainer* resultContainerLoaded =
            (resultContainers == nullptr) ? (nullptr) :
                                            (resultContainers[tableReference]);
    const Table& joinTableLoaded = tableLoader.getTable(tables[tableReference]);
    const uint64_t* tableCols[colsToProcessNum];
    uint32_t payloadTables[colsToProcessNum];
    for (uint32_t i = 0; i < colsToProcessNum; ++i) {
        const TableColumn& currTableColumn = colsToProcess[i];
        if (resultContainerLoaded == nullptr
            || currTableColumn.getTableNum() == tableReference) {
            tableCols[i] = joinTableLoaded.getCol(currTableColumn.getTableCol());
            payloadTables[i] = tableReference;
        }
        else {
            tableCols[i] = tableLoader.getTable(tables[payloadTables[i] = currTableColumn.getTableNum()]).getCol(currTableColumn.getTableCol());
        }
    }
    //If this table has not been previously processed
    if (resultContainerLoaded == nullptr) {
        //Find filters
        uint32_t filtersToApplyNum = 0;
        for (uint32_t filterIndex = 0; filterIndex < filterNum; ++filterIndex) {
            const Filter* currFilter = filters[filterIndex];
            if (currFilter->getTable() == tableReference) {
                filtersToApplyNum++;
            }
        }
        const Filter* filtersToApply[filtersToApplyNum];
        filtersToApplyNum = 0;
        for (uint32_t filterIndex = 0; filterIndex < filterNum; ++filterIndex) {
            const Filter* currFilter = filters[filterIndex];
            if (currFilter->getTable() == tableReference) {
                filtersToApply[filtersToApplyNum++] = currFilter;
            }
        }

        //Load data
        const uint64_t rows = joinTableLoaded.getRows();
        Relation retVal(rows, tableNum, colsToProcessNum);
        retVal.setUsedRow(tableReference);
        for (uint64_t currRowNum = 0; currRowNum < rows; currRowNum++) {
            //Apply filters for this table
            bool failedFilters = false;
            for (uint32_t filterIndex = 0; filterIndex < filtersToApplyNum;
                    ++filterIndex) {
                const Filter& currFilter = *(filtersToApply[filterIndex]);
                if (!currFilter.passesFilter(joinTableLoaded, currRowNum)) {
                    failedFilters = true;
                    break;
                }
            }
            if (failedFilters) {
                continue;
            }

            //If row passes filters, load it
            Tuple toAdd(tableNum, colsToProcessNum);
            toAdd.setTableRow(tableReference, currRowNum);
            for (size_t j = 0; j < colsToProcessNum; ++j) {
                toAdd.setPayload(j, tableCols[j][currRowNum]);
            }
            CO_IFDEBUG(consoleOutput, "Adding Tuple "<<toAdd);
            retVal.addTuple(move(toAdd));
        }
        return retVal;
    }
    //Else, if resultContainerLoaded is not null (i.e. table was part of a previously joined relation)
    return resultContainerLoaded->loadToRelation(colsToProcessNum,
                                                 tableCols,
                                                 payloadTables);
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
