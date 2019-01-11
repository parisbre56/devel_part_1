/*
 * JoinTree.h
 *
 *  Created on: 27 Δεκ 2018
 *      Author: parisbre56
 */

#ifndef JOINORDER_H_
#define JOINORDER_H_
class JoinOrder;

#include <ostream>

#include "MultipleColumnStats.h"

class JoinOrder {
protected:
    uint32_t arraySize;
    uint32_t orderedTables;
    uint32_t* tableOrder;
    const MultipleColumnStats** stats;
    uint64_t* rowSum;
public:
    explicit JoinOrder(uint32_t tables);
    JoinOrder(uint32_t tables,
              uint32_t table,
              const MultipleColumnStats& tableStats);
    JoinOrder(const JoinOrder& toCopy);
    JoinOrder(JoinOrder&& toMove);
    JoinOrder& operator=(const JoinOrder& toCopy);
    JoinOrder& operator=(JoinOrder&& toMove);
    virtual ~JoinOrder();

    void addTable(uint32_t table, const MultipleColumnStats& tableStats);
    /** Create a copy to this new table, add the given table to the copy and return the copy **/
    JoinOrder addTableNew(uint32_t table,
                          const MultipleColumnStats& tableStats) const;
    bool containsTable(uint32_t table) const;
    bool sameSet(const JoinOrder& other) const;

    uint32_t getArraySize() const;
    uint32_t getOrderedTables() const;
    const uint32_t* getTableOrder() const;
    const MultipleColumnStats* const * getStats() const;
    const uint64_t* getRowSum() const;

    friend std::ostream& operator<<(std::ostream& os, const JoinOrder& toPrint);
};

#endif /* JOINORDER_H_ */
