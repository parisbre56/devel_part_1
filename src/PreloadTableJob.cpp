/*
 * PreloadTableJob.cpp
 *
 *  Created on: 9 Ιαν 2019
 *      Author: pibre
 */

#include "PreloadTableJob.h"

#include "ConsoleOutput.h"

using namespace std;

PreloadTableJob::PreloadTableJob(const TableLoader& tableLoader,
                                 const uint32_t tableReference,
                                 const uint32_t tableNum,
                                 const uint32_t* const tables,
                                 const uint32_t filterNum,
                                 const Filter* const * const filters,
                                 uint32_t sameTableRelations) :
        tableLoader(tableLoader),
        tableReference(tableReference),
        tableNum(tableNum),
        tables(tables),
        filterNum(filterNum),
        filters(filters),
        colsToProcessNum(sameTableRelations),
        colsToProcess(
                (sameTableRelations == 0) ? (nullptr) :
                                            (new TableColumn[sameTableRelations])),
        result(nullptr) {
}

PreloadTableJob::~PreloadTableJob() {
    if (result != nullptr) {
        delete result;
    }
    if (colsToProcess != nullptr) {
        delete[] colsToProcess;
    }
}

void PreloadTableJob::printSelf(ostream& os) const {
    os << "[PreloadTableJob finished="
       << finished
       << ",tableReference="
       << tableReference
       << ", colsToProcessNum="
       << colsToProcessNum
       << ", colsToProcess=";
    if (colsToProcess == nullptr) {
        os << "null";
    }
    else {
        os << "[";
        for (uint32_t i = 0; i < colsToProcessNum; ++i) {
            if (i != 0) {
                os << ", ";
            }
            os << colsToProcess[i];
        }
        os << "]";
    }
    os << "]";
}

Relation * PreloadTableJob::getResultInternal() {
    return result;
}

void PreloadTableJob::runInternal() {
    ConsoleOutput consoleOutput("PreloadTableJob::runInternal");
    const Table& joinTableLoaded = tableLoader.getTable(tables[tableReference]);
    const uint64_t* tableCols[colsToProcessNum];
    for (uint32_t i = 0; i < colsToProcessNum; ++i) {
        const TableColumn& currTableColumn = colsToProcess[i];
        tableCols[i] = joinTableLoaded.getCol(currTableColumn.getTableCol());
    }

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
    result = new Relation(joinTableLoaded.getRows(),
                          tableNum,
                          colsToProcessNum);
    result->setUsedRow(tableReference);
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
        result->addTuple(move(toAdd));
    }
}

bool PreloadTableJob::failsFilters(uint32_t filtersToApplyNum,
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

void PreloadTableJob::setColToProcess(uint32_t sameTableRelationsIndex,
                                      uint32_t tableNum,
                                      size_t tableCol) {
    colsToProcess[sameTableRelationsIndex].setTableNum(tableNum);
    colsToProcess[sameTableRelationsIndex].setTableCol(tableCol);
}
ostream& operator<<(ostream& os, const PreloadTableJob& toPrint) {
    toPrint.printSelf(os);
    return os;
}
