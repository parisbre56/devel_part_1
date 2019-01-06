/*
 * Join.h
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#ifndef JOIN_H_
#define JOIN_H_
class Join;

#include "TableLoader.h"
#include "Filter.h"
#include "FilterGreater.h"
#include "FilterEquals.h"
#include "FilterLesser.h"
#include "JoinRelation.h"
#include "TableColumn.h"
#include "JoinSumResult.h"
#include "Relation.h"
#include "ResultContainer.h"
#include "MultipleColumnStats.h"
#include "JoinOrder.h"
#include "JoinOrderContainer.h"
#include "Executor.h"

class Join {
protected:
    const TableLoader& tableLoader;
    Executor& executor;
    const uint32_t arraySize;
    uint32_t tableNum;
    uint32_t* tables;
    uint32_t filterNum;
    const Filter** filters;
    uint32_t joinRelationNum;
    const JoinRelation** joinRelations;
    uint32_t sumColumnNum;
    const TableColumn** sumColumns; //TODO reorder to bring same table closer?
    //Current results of joining, each relation is stored in its own resultContainer
    ResultContainer** resultContainers;
    MultipleColumnStats** tableStats;
    JoinOrderContainer* oldOrder;
    JoinOrderContainer* newOrder;

    Relation loadRelation(const uint32_t tableReference,
                          const uint32_t colsToProcessNum,
                          const TableColumn* const colsToProcess) const;
    ResultContainer radixHashJoin(const Relation& relR,
                                  const Relation& relS) const;
    void storeResut(ResultContainer* newResult);
    void fillSums(JoinSumResult& retVal) const;
    void fillSumsFromRelation(JoinSumResult& retVal,
                              const Relation& currRel,
                              const uint64_t * const * const sumCols,
                              const uint32_t * const sumTable) const;
    void fillSumTables(const uint64_t* * const sumCols,
                       uint32_t * const sumTable) const;
    unsigned char getBitmaskSize(const uint64_t rows) const;
    uint32_t getBucketAndChainBuckets(const uint64_t tuplesInBucket) const;
    MultipleColumnStats loadStats(const uint32_t table) const;
    /** Handle joining two tables. Two first args are the retvals **/
    void updateJoinStats(MultipleColumnStats& newStat,
                         bool& disconnected,
                         const JoinOrder& currentSubset,
                         size_t preJoinCols,
                         uint32_t currTable,
                         size_t currCol,
                         uint32_t joinTable,
                         size_t joinCol);
    /** Tells us how many col stats we need to skip to reach the stats for the given table,
     * assuming the stat col order follows the same order as the given JoinOrder **/
    size_t colOffsetForTable(const JoinOrder& currentSubset,
                             uint32_t joinTable);

public:
    Join() = delete;
    Join(const TableLoader& tableLoader,
         Executor& executor,
         uint32_t arraySize);
    Join(const Join& toCopy) = delete;
    Join(Join&& toMove) = delete;
    Join& operator=(const Join& toCopy) = delete;
    Join& operator=(Join&& toMove) = delete;
    virtual ~Join();

    void addTable(uint32_t table);
    void addTableRelationship(uint32_t tableA,
                              size_t columnA,
                              uint32_t tableB,
                              size_t columnB);
    void addTableFilter(uint32_t table,
                        size_t column,
                        uint64_t filterNumber,
                        char type);
    void addSumColumn(uint32_t table, size_t column);
    JoinSumResult performJoin();

    friend std::ostream& operator<<(std::ostream& os, const Join& toPrint);
};

#endif /* JOIN_H_ */
