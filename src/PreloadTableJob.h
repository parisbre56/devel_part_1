/*
 * PreloadTableJob.h
 *
 *  Created on: 9 Ιαν 2019
 *      Author: pibre
 */

#ifndef PRELOADTABLEJOB_H_
#define PRELOADTABLEJOB_H_
class PreloadTableJob;

#include "Callable.h"
#include "JoinRelation.h"
#include "TableColumn.h"
#include "Filter.h"
#include "TableLoader.h"

class PreloadTableJob: public Callable<Relation> {
protected:
    const TableLoader& tableLoader;
    const uint32_t tableReference;
    const uint32_t tableNum;
    const uint32_t* const tables;
    const uint32_t filterNum;
    const Filter* const * const filters;
    const uint32_t colsToProcessNum;
    TableColumn* const colsToProcess;
    Relation* result;

    void printSelf(std::ostream& os) const;
    Relation * getResultInternal();
    void runInternal();
    bool failsFilters(uint32_t filtersToApplyNum,
                      const Filter* const * const filtersToApply,
                      const Table& joinTableLoaded,
                      uint64_t currRowNum) const;
public:
    PreloadTableJob(const TableLoader& tableLoader,
                    const uint32_t tableReference,
                    const uint32_t tableNum,
                    const uint32_t* const tables,
                    const uint32_t filterNum,
                    const Filter* const * const filters,
                    uint32_t sameTableRelations);
    PreloadTableJob(const PreloadTableJob& toCopy) = delete;
    PreloadTableJob(PreloadTableJob&& toMove) = delete;
    PreloadTableJob& operator=(const PreloadTableJob& toCopy) = delete;
    PreloadTableJob& operator=(PreloadTableJob&& toMove) = delete;
    virtual ~PreloadTableJob();

    void setColToProcess(uint32_t sameTableRelationsIndex,
                         uint32_t tableNum,
                         size_t tableCol);

    friend std::ostream& operator<<(std::ostream& os,
                                    const PreloadTableJob& toPrint);
};

#endif /* PRELOADTABLEJOB_H_ */
