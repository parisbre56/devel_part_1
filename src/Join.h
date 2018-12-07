/*
 * Join.h
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#ifndef JOIN_H_
#define JOIN_H_

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

class Join {
protected:
    const TableLoader& tableLoader;
    const uint32_t arraySize;
    uint32_t tableNum;
    uint32_t* tables;
    uint32_t filterNum;
    const Filter** filters;
    uint32_t joinRelationNum;
    const JoinRelation** joinRelations;
    uint32_t sumColumnNum;
    const TableColumn** sumColumns;
    //Current results of joining, each relation is stored in its own resultContainer
    ResultContainer** resultContainers;

    const JoinRelation* findSmallestRelation(const bool* const isRelationProcessed,
                                       const ResultContainer* const * const resultLocation,
                                       uint32_t& smallestRelationIndex,
                                       uint32_t& sameTableRelations) const;
    Relation loadRelation(const uint32_t tableReference,
                          const uint32_t colsToProcessNum,
                          const size_t* const colsToProcess,
                          const ResultContainer* const * const resultLocation) const;
    ResultContainer radixHashJoin(const Relation& relR,
                                  const Relation& relS) const;

public:
    Join() = delete;
    Join(const TableLoader& tableLoader, uint32_t arraySize);
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
