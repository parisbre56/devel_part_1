/*
 * Join.cpp
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#include "Join.h"

#include <limits>
#include <sstream>

#include "ResultContainer.h"
#include "ConsoleOutput.h"
#include "Relation.h"
#include "HashTableJob.h"
#include "BucketAndChain.h"
#include "FilterSameTable.h"
#include "HashFunctionBitmask.h"
#include "HashFunctionModulo.h"

using namespace std;

Join::Join(const TableLoader& tableLoader,
           Executor& hashExecutor,
           Executor& joinExecutor,
           Executor& preloadExecutor,
           uint32_t arraySize) :
        tableLoader(tableLoader),
        hashExecutor(hashExecutor),
        joinExecutor(joinExecutor),
        preloadExecutor(preloadExecutor),
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
        tableStats(nullptr),
        oldOrder(nullptr),
        newOrder(nullptr),
        buckets(0),
        joinJobs(nullptr),
        sumJobs(nullptr),
        preloadTableJobs(nullptr) {

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
    if (tableStats != nullptr) {
        for (uint32_t i = 0; i < tableNum; ++i) {
            if (tableStats[i] != nullptr) {
                delete tableStats[i];
            }
        }
    }
    if (oldOrder != nullptr) {
        delete oldOrder;
    }
    if (newOrder != nullptr) {
        delete newOrder;
    }
    if (joinJobs != nullptr) {
        for (uint32_t i = 0; i < buckets; ++i) {
            if (joinJobs[i] != nullptr) {
                delete joinJobs[i];
            }
        }
        delete[] joinJobs;
    }
    if (sumJobs != nullptr) {
        for (uint32_t i = 0; i < sumColumnNum; ++i) {
            if (sumJobs[i] != nullptr) {
                delete sumJobs[i];
            }
        }
        delete[] sumJobs;
    }
    if (preloadTableJobs != nullptr) {
        for (uint32_t i = 0; i < tableNum; ++i) {
            if (preloadTableJobs[i] != nullptr) {
                preloadTableJobs[i]->waitToFinish(); //We have no way to cancel jobs, so we are obliged to let it finish or risk segfault
                delete preloadTableJobs[i];
            }
        }
        delete[] preloadTableJobs;
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

size_t Join::colOffsetForTable(const JoinOrder& currentSubset,
                               uint32_t joinTable) {
    size_t colSum = 0;
    for (uint32_t orderedTableIndex = 0;
            orderedTableIndex < currentSubset.getOrderedTables();
            ++orderedTableIndex) {
        const uint32_t currOrderedTable = currentSubset.getTableOrder()[orderedTableIndex];
        if (currOrderedTable == joinTable) {
            break;
        }
        colSum += tableStats[currOrderedTable]->getCols();
    }
    return colSum;
}

void Join::updateJoinStats(MultipleColumnStats& newStat,
                           bool& disconnected,
                           const JoinOrder& currentSubset,
                           size_t preJoinCols,
                           uint32_t currTable,
                           size_t currCol,
                           uint32_t joinTable,
                           size_t joinCol) {
    if (disconnected) {
        disconnected = false;
        newStat = newStat.join(colOffsetForTable(currentSubset, joinTable)
                               + joinCol,
                               *(tableStats[currTable]),
                               joinCol);
    }
    //If we've already joined, then it's the same as applying a same table filter
    else {
        newStat = newStat.filterSame(colOffsetForTable(currentSubset, joinTable)
                                     + joinCol,
                                     preJoinCols + currCol);
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
    tableStats = new MultipleColumnStats*[tableNum] {/* init to nullptr */};

    //Apply filters to all table stats
    newOrder = new JoinOrderContainer(tableNum);
    for (uint32_t i = 0; i < tableNum; ++i) {
        tableStats[i] = new MultipleColumnStats(loadStats(i));
        MultipleColumnStats stats(*(tableStats[i]));
        JoinOrder toAdd(tableNum, i, stats);
        newOrder->addIfBetterMove(move(toAdd), move(stats));
    }
    //Find joinOrder
    JoinOrderContainer finalOrder(tableNum);
    for (uint32_t permutationSize = 1; permutationSize < tableNum;
            ++permutationSize) {
        //TODO reset and cycle for better performance?
        oldOrder = newOrder;
        newOrder = new JoinOrderContainer(oldOrder->getUsed() * 2);
        for (uint32_t subsetIndex = 0; subsetIndex < oldOrder->getUsed();
                ++subsetIndex) {
            const JoinOrder& currentSubset = *((oldOrder->getJoinOrders())[subsetIndex]);
            const MultipleColumnStats& currentStat = *((oldOrder->getStats())[subsetIndex]);
            bool notAdded = true;
            for (uint32_t toAdd = 0; toAdd < tableNum; ++toAdd) {
                //Skip tables that are already processed in this subset
                if (currentSubset.containsTable(toAdd)) {
                    continue;
                }
                //Compute new stats
                MultipleColumnStats newStat(currentStat);
                bool disconnected = true;
                const size_t preJoinCols = newStat.getCols();
                for (uint32_t joinIndex = 0; joinIndex < joinRelationNum;
                        joinIndex++) {
                    const JoinRelation& currRelation = *(joinRelations[joinIndex]);
                    if (currRelation.getLeftNum() == toAdd) {
                        if (currentSubset.containsTable(currRelation.getRightNum())) {
                            updateJoinStats(newStat,
                                            disconnected,
                                            currentSubset,
                                            preJoinCols,
                                            toAdd,
                                            currRelation.getLeftCol(),
                                            currRelation.getRightNum(),
                                            currRelation.getRightCol());
                        }
                    }
                    else if (currRelation.getRightNum() == toAdd) {
                        if (currentSubset.containsTable(currRelation.getLeftNum())) {
                            updateJoinStats(newStat,
                                            disconnected,
                                            currentSubset,
                                            preJoinCols,
                                            toAdd,
                                            currRelation.getRightCol(),
                                            currRelation.getLeftNum(),
                                            currRelation.getLeftCol());
                        }
                    }
                }
                //Skip disconnected tables
                if (disconnected) {
                    continue;
                }
                notAdded = false;
                //Add new subset if better
                newOrder->addIfBetterMove(currentSubset.addTableNew(toAdd,
                                                                    newStat),
                                          move(newStat));
            }
            //Disconnected tables need to be added in case of cartesian products
            if (notAdded && currentSubset.getOrderedTables() > 1) {
                finalOrder.stealEntry(*oldOrder, subsetIndex);
            }
        }
        //We no longer need the old order, delete it
        delete oldOrder;
        oldOrder = nullptr;
        //No use continuing if we have nothing to do. Stuff should have been added into the final order
        if (newOrder->getUsed() == 0) {
            break;
        }
    }
    CO_IFDEBUG(consoleOutput, "newOrder="<<*newOrder);

    //Everything left in the newOrder (either 1 or 0 entries) should be moved to the final order
    for (uint32_t subsetIndex = 0; subsetIndex < newOrder->getUsed();
            ++subsetIndex) {
        finalOrder.stealEntry(*newOrder, subsetIndex);
    }
    CO_IFDEBUG(consoleOutput, "finalOrder="<<finalOrder);

    //Now that we've found the join order we no longer need these things, so free up some space
    delete newOrder;
    newOrder = nullptr;
    for (uint32_t i = 0; i < tableNum; ++i) {
        if (tableStats[i] != nullptr) {
            delete tableStats[i];
            tableStats[i] = nullptr;
        }
    }
    delete[] tableStats;
    tableStats = nullptr;

    //Preload tables
    const uint32_t finalOrderUsed = finalOrder.getUsed();
    {
        bool isRelationProcessedPreload[joinRelationNum] { /* init to false */};
        preloadTableJobs = new PreloadTableJob*[tableNum] {/* init to nullptr */};
        for (uint32_t subsetIndex = 0; subsetIndex < finalOrderUsed;
                ++subsetIndex) {
            const JoinOrder& currentSubset = *((finalOrder.getJoinOrders())[subsetIndex]);
            CO_IFDEBUG(consoleOutput,
                       "Processing subset [subsetIndex="<<subsetIndex<<", currentSubset="<<currentSubset<<"]");
            const uint32_t subsetOrderedTablesNum = currentSubset.getOrderedTables();
            //If this is a disconnected table, then preload it without payload
            if (subsetOrderedTablesNum == 1) {
                uint32_t tableToLoad = currentSubset.getTableOrder()[0];
                preloadTableJobs[tableToLoad] = new PreloadTableJob(tableLoader,
                                                                    tableToLoad,
                                                                    tableNum,
                                                                    tables,
                                                                    filterNum,
                                                                    filters,
                                                                    0);
                preloadExecutor.addToQueue(preloadTableJobs[currentSubset.getTableOrder()[0]]);
                continue;
            }
            //Else, preload with the necessary payload
            for (uint32_t subsetTableIndex = 1;
                    subsetTableIndex < currentSubset.getOrderedTables();
                    ++subsetTableIndex) {
                CO_IFDEBUG(consoleOutput,
                           "Processing subset [subsetTableIndex="<<subsetTableIndex<<", subsetIndex="<<subsetIndex<<", currentSubset="<<currentSubset<<"]");
                uint32_t sameTableRelations = 0;
                const JoinRelation* smallestRelation = nullptr;
                for (uint32_t relationIndex = 0;
                        relationIndex < joinRelationNum; ++relationIndex) {
                    if (isRelationProcessedPreload[relationIndex]) {
                        continue;
                    }
                    const JoinRelation& currRelation = *(joinRelations[relationIndex]);
                    if (currRelation.getLeftNum()
                        == currentSubset.getTableOrder()[subsetTableIndex]) {
                        bool matchesSubset = false;
                        for (uint32_t i = 0; i < subsetTableIndex; ++i) {
                            if (currentSubset.getTableOrder()[i]
                                == currRelation.getRightNum()) {
                                matchesSubset = true;
                                break;
                            }
                        }
                        if (matchesSubset) {
                            ++sameTableRelations;
                            if (smallestRelation == nullptr) {
                                smallestRelation = joinRelations[relationIndex];
                            }
                        }
                    }
                    else if (currRelation.getRightNum()
                             == currentSubset.getTableOrder()[subsetTableIndex]) {
                        bool matchesSubset = false;
                        for (uint32_t i = 0; i < subsetTableIndex; ++i) {
                            if (currentSubset.getTableOrder()[i]
                                == currRelation.getLeftNum()) {
                                matchesSubset = true;
                                break;
                            }
                        }
                        if (matchesSubset) {
                            ++sameTableRelations;
                            if (smallestRelation == nullptr) {
                                smallestRelation = joinRelations[relationIndex];
                            }
                        }
                    }
                }

                if (smallestRelation == nullptr) {
                    throw runtime_error("Should never happen: Relation not found");
                }
                CO_IFDEBUG(consoleOutput,
                           "Found relations [sameTableRelations="<<sameTableRelations<<", smallestRelation=" << (*smallestRelation) <<"]");

                const uint32_t tableToLoad = currentSubset.getTableOrder()[subsetTableIndex];
                const uint32_t leftNum = smallestRelation->getLeftNum();
                const uint32_t rightNum = smallestRelation->getRightNum();
                const bool processLeft = (subsetTableIndex == 1
                                          || leftNum == tableToLoad);
                const bool processRight = (subsetTableIndex == 1
                                           || rightNum == tableToLoad);
                if (processLeft) {
                    preloadTableJobs[leftNum] = new PreloadTableJob(tableLoader,
                                                                    leftNum,
                                                                    tableNum,
                                                                    tables,
                                                                    filterNum,
                                                                    filters,
                                                                    sameTableRelations);
                }
                if (processRight) {
                    preloadTableJobs[rightNum] = new PreloadTableJob(tableLoader,
                                                                     rightNum,
                                                                     tableNum,
                                                                     tables,
                                                                     filterNum,
                                                                     filters,
                                                                     sameTableRelations);
                }

                sameTableRelations = 0;
                for (uint32_t relationIndex = 0;
                        relationIndex < joinRelationNum; ++relationIndex) {
                    const JoinRelation& currRelation = *(joinRelations[relationIndex]);
                    unsigned char cmp = currRelation.sameJoinAs(*smallestRelation,
                                                                resultContainers);
                    if (cmp != 0) {
                        CO_IFDEBUG(consoleOutput,
                                   "Will process relation [cmp=" << to_string(cmp) << ", currRelation=" << currRelation << "]");
                        isRelationProcessedPreload[relationIndex] = true;
                        if (cmp == 1) {
                            if (processLeft) {
                                preloadTableJobs[leftNum]->setColToProcess(sameTableRelations,
                                                                           currRelation.getLeftNum(),
                                                                           currRelation.getLeftCol());
                            }
                            if (processRight) {
                                preloadTableJobs[rightNum]->setColToProcess(sameTableRelations,
                                                                            currRelation.getRightNum(),
                                                                            currRelation.getRightCol());
                            }
                        }
                        else if (cmp == 2) {
                            if (processLeft) {
                                preloadTableJobs[leftNum]->setColToProcess(sameTableRelations,
                                                                           currRelation.getRightNum(),
                                                                           currRelation.getRightCol());
                            }
                            if (processRight) {
                                preloadTableJobs[rightNum]->setColToProcess(sameTableRelations,
                                                                            currRelation.getLeftNum(),
                                                                            currRelation.getLeftCol());
                            }
                        }
                        else {
                            throw runtime_error("Unknown cmp while searching for matching relations");
                        }
                        sameTableRelations++;
                    }
                }

                if (processLeft) {
                    preloadExecutor.addToQueue(preloadTableJobs[leftNum]);
                }
                if (processRight) {
                    preloadExecutor.addToQueue(preloadTableJobs[rightNum]);
                }
            }
        }
    }

    //Start joining in the orders we've found
    for (uint32_t subsetIndex = 0; subsetIndex < finalOrderUsed;
            ++subsetIndex) {
        const JoinOrder& currentSubset = *((finalOrder.getJoinOrders())[subsetIndex]);
        CO_IFDEBUG(consoleOutput,
                   "Processing subset [subsetIndex="<<subsetIndex<<", currentSubset="<<currentSubset<<"]");
        {
            stringstream ss;
            ss << "Join start  [usedTables=[";
            for (uint32_t i = 0; i < tableNum; ++i) {
                if (i != 0) {
                    ss << ", ";
                }
                if (i == currentSubset.getTableOrder()[0]) {
                    ss << 1;
                }
                else {
                    ss << 0;
                }
            }
            ss << "], rows="
               << tableLoader.getTable(tables[currentSubset.getTableOrder()[0]]).getRows()
               << ", predictedRows="
               << currentSubset.getStats()[0]->getColumnStats()[0].getTotalRows()
               << ", rowSum="
               << currentSubset.getRowSum()[0]
               << "]";
            cerr << ss.str() << endl;
        }
        for (uint32_t subsetTableIndex = 1;
                subsetTableIndex < currentSubset.getOrderedTables();
                ++subsetTableIndex) {
            CO_IFDEBUG(consoleOutput,
                       "Processing subset [subsetTableIndex="<<subsetTableIndex<<", subsetIndex="<<subsetIndex<<", currentSubset="<<currentSubset<<"]");
            uint32_t sameTableRelations = 0;
            const JoinRelation* smallestRelation = nullptr;
            for (uint32_t relationIndex = 0; relationIndex < joinRelationNum;
                    ++relationIndex) {
                if (isRelationProcessed[relationIndex]) {
                    continue;
                }
                const JoinRelation& currRelation = *(joinRelations[relationIndex]);
                if (currRelation.getLeftNum()
                    == currentSubset.getTableOrder()[subsetTableIndex]) {
                    bool matchesSubset = false;
                    for (uint32_t i = 0; i < subsetTableIndex; ++i) {
                        if (currentSubset.getTableOrder()[i]
                            == currRelation.getRightNum()) {
                            matchesSubset = true;
                            break;
                        }
                    }
                    if (matchesSubset) {
                        ++sameTableRelations;
                        if (smallestRelation == nullptr) {
                            smallestRelation = joinRelations[relationIndex];
                        }
                    }
                }
                else if (currRelation.getRightNum()
                         == currentSubset.getTableOrder()[subsetTableIndex]) {
                    bool matchesSubset = false;
                    for (uint32_t i = 0; i < subsetTableIndex; ++i) {
                        if (currentSubset.getTableOrder()[i]
                            == currRelation.getLeftNum()) {
                            matchesSubset = true;
                            break;
                        }
                    }
                    if (matchesSubset) {
                        ++sameTableRelations;
                        if (smallestRelation == nullptr) {
                            smallestRelation = joinRelations[relationIndex];
                        }
                    }
                }
            }

            if (smallestRelation == nullptr) {
                throw runtime_error("Should never happen: Relation not found");
            }
            CO_IFDEBUG(consoleOutput,
                       "Found relations [sameTableRelations="<<sameTableRelations<<", smallestRelation=" << (*smallestRelation) <<"]");

            TableColumn colsToProcessLeft[sameTableRelations];
            TableColumn colsToProcessRight[sameTableRelations];
            sameTableRelations = 0;
            for (uint32_t relationIndex = 0; relationIndex < joinRelationNum;
                    ++relationIndex) {
                const JoinRelation& currRelation = *(joinRelations[relationIndex]);
                unsigned char cmp = currRelation.sameJoinAs(*smallestRelation,
                                                            resultContainers);
                if (cmp != 0) {
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
                {
                    stringstream ss;
                    ss << "Join result [usedTables=[";
                    for (uint32_t i = 0; i < tableNum; ++i) {
                        if (i != 0) {
                            ss << ", ";
                        }
                        ss << (relL.getUsedRow(i) || relR.getUsedRow(i));
                    }
                    ss << "], rows="
                       << 0
                       << ", predictedRows="
                       << currentSubset.getStats()[subsetTableIndex]->getColumnStats()[0].getTotalRows()
                       << ", rowSum="
                       << currentSubset.getRowSum()[subsetTableIndex]
                       << "]";
                    cerr << ss.str() << endl;
                }
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
            {
                stringstream ss;
                ss << "Join result [usedTables=[";
                for (uint32_t i = 0; i < tableNum; ++i) {
                    if (i != 0) {
                        ss << ", ";
                    }
                    ss << newResult->getUsedRows()[i];
                }
                ss << "], rows="
                   << newResult->getResultCount()
                   << ", predictedRows="
                   << currentSubset.getStats()[subsetTableIndex]->getColumnStats()[0].getTotalRows()
                   << ", rowSum="
                   << currentSubset.getRowSum()[subsetTableIndex]
                   << "]";
                cerr << ss.str() << endl;
            }
            storeResut(newResult);

            //Empty join means no sum
            if (newResult->getResultCount() == 0) {
                CO_IFDEBUG(consoleOutput, "Result empty, not performing join");
                return retVal;
            }
            CO_IFDEBUG(consoleOutput,
                       "Finished processing relations for table for subset")
        }
        CO_IFDEBUG(consoleOutput, "Finished processing tables for subset");
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
                   "Cartesian product [smallestResultIndex=" << smallestResultIndex << ", secondSmallestResultIndex=" << secondSmallestResultIndex << ", smallestResult=" << *smallestResult << ", secondSmallestResult=" << *secondSmallestResult << "]");
        Relation relR(loadRelation(smallestResultIndex, 0, nullptr));
        Relation relS(loadRelation(secondSmallestResultIndex, 0, nullptr));
        CO_IFDEBUG(consoleOutput,
                   "Cartesian product relations [relR="<<relR<<", relS="<<relS<<"]");
        ResultContainer* newResult = new ResultContainer(relR * relS);
        CO_IFDEBUG(consoleOutput,
                   "Cartesian product finished [result="<<*newResult<<"]");
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

void Join::fillSums(JoinSumResult& retVal) {
    if (resultContainers[0]->getResultCount() == 0) {
        return;
    }
    //Start from the first non-empty results
    const Result* startResult = resultContainers[0]->getFirstResultBlock();
    while (startResult != nullptr
           && startResult->getRelation().getNumTuples() == 0) {
        startResult = startResult->getNext();
    }
    if (startResult == nullptr) {
        return;
    }
    retVal.setHasResults();

    //Recursively compute the sum for each result
    sumJobs = new SumJob*[sumColumnNum] {/*init to nullptr*/};
    for (uint32_t sumIndex = 0; sumIndex < sumColumnNum; ++sumIndex) {
        const TableColumn& currSumColumn = *(sumColumns[sumIndex]);
        sumJobs[sumIndex] = new SumJob(joinExecutor,
                                       *startResult,
                                       currSumColumn.getTableNum(),
                                       tableLoader.getTable(tables[currSumColumn.getTableNum()]).getCol(currSumColumn.getTableCol()));
        joinExecutor.addToQueue(sumJobs[sumIndex]);
    }
    for (uint32_t sumIndex = 0; sumIndex < sumColumnNum; ++sumIndex) {
        retVal.addSum(sumIndex, *(sumJobs[sumIndex]->waitAndGetResult()));
        delete sumJobs[sumIndex];
        sumJobs[sumIndex] = nullptr;
    }
    delete[] sumJobs;
    sumJobs = nullptr;
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
                                    const Relation& relS) {
    ConsoleOutput consoleOutput("RadixHashJoin");
//consoleOutput.errorOutput() << "JOIN EXECUTION STARTED" << endl;

    HashFunctionBitmask bitmask(getBitmaskSize(relR.getNumTuples()));

    HashTableJob rHash(hashExecutor, relR, bitmask);
    joinExecutor.addToQueue(&rHash);
    HashTableJob sHash(hashExecutor, relS, bitmask);
    joinExecutor.addToQueue(&sHash);
    //CO_IFDEBUG(consoleOutput, "Hashes generated");
    //CO_IFDEBUG(consoleOutput, "rHash=" << rHash);
    //CO_IFDEBUG(consoleOutput, "sHash=" << sHash);

    ResultContainer retResult(0, relR.getSizeTableRows(), 0);
    for (uint32_t i = 0; i < tableNum; ++i) {
        if (relR.getUsedRow(i) || relS.getUsedRow(i)) {
            retResult.setUsedRow(i);
        }
    }
    buckets = bitmask.getBuckets();
    joinJobs = new JoinJob*[buckets] {/* init to nullptr */};
    for (uint32_t i = 0; i < buckets; ++i) {
        joinJobs[i] = new JoinJob(rHash, sHash, i, retResult.getUsedRows());
        joinExecutor.addToQueue(joinJobs[i]);
    }
    for (uint32_t i = 0; i < buckets; ++i) {
        ResultContainer& toMerge = *(joinJobs[i]->waitAndGetResult());
        if (toMerge.getResultCount() == 0) {
            CO_IFDEBUG(consoleOutput,
                       "Skipping job with empty result [bucket="<<i<<", joinJob="<<*(joinJobs[i])<<", toMerge="<<toMerge<<"]");
        }
        else {
            retResult.mergeResult(move(toMerge));
        }
        delete joinJobs[i];
        joinJobs[i] = nullptr;
    }
    delete[] joinJobs;
    joinJobs = nullptr;

    //consoleOutput.errorOutput() << "JOIN EXECUTION ENDED" << endl;
    return retResult;
}

bool Join::failsFilters(uint32_t filtersToApplyNum,
                        const Filter* const * const filtersToApply,
                        const Table& joinTableLoaded,
                        uint64_t currRowNum) const {
    for (uint32_t filterIndex = 0; filterIndex < filtersToApplyNum;
            ++filterIndex) {
        const Filter& currFilter = *(filtersToApply[filterIndex]);
        if (!currFilter.passesFilter(joinTableLoaded, currRowNum)) {
            return true;
        }
    }
    return false;
}

Relation Join::loadRelation(const uint32_t tableReference,
                            const uint32_t colsToProcessNum,
                            const TableColumn* const colsToProcess) const {
    ConsoleOutput consoleOutput("Join::loadRelation");
    if (preloadTableJobs != nullptr
        && preloadTableJobs[tableReference] != nullptr) {
        CO_IFDEBUG(consoleOutput, "Waiting for preloaded table");
        Relation& preloadedResult = *(preloadTableJobs[tableReference]->waitAndGetResult());
        Relation preloaded(move(preloadedResult));
        delete preloadTableJobs[tableReference];
        preloadTableJobs[tableReference] = nullptr;
        return preloaded;
    }
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
            //Skip row if it fails filters
            if (failsFilters(filtersToApplyNum,
                             filtersToApply,
                             joinTableLoaded,
                             currRowNum)) {
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
    return resultContainerLoaded->loadToRelation(hashExecutor,
                                                 colsToProcessNum,
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
