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
    Filter** filters;
    uint32_t joinRelationNum;
    JoinRelation** joinRelations;
    uint32_t sumColumnNum;
    TableColumn** sumColumns;

    JoinRelation* findSmallestRelation(const bool* const isRelationProcessed,
                                       const ResultContainer* const * const resultLocation,
                                       uint32_t& smallestRelationIndex) const;
    Relation loadRelation(const uint32_t tableReference,
                          const size_t tableCol,
                          const ResultContainer* const * const resultLocation,
                          const bool* const resultKey) const;
    uint32_t hashFunc(uint32_t buckets, uint64_t toHash) const;
    uint32_t hashFuncChain(uint32_t buckets, uint64_t toHash) const;
    ResultContainer radixHashJoin(Relation& relR, Relation& relS) const;

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
