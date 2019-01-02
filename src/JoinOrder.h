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

class JoinOrder {
protected:
    uint32_t arraySize;
    uint32_t orderedTables;
    uint32_t* tableOrder;
public:
    explicit JoinOrder(uint32_t tables);
    JoinOrder(uint32_t tables, uint32_t table);
    JoinOrder(const JoinOrder& toCopy);
    JoinOrder(JoinOrder&& toMove);
    JoinOrder& operator=(const JoinOrder& toCopy);
    JoinOrder& operator=(JoinOrder&& toMove);
    virtual ~JoinOrder();

    void addTable(uint32_t table);
    /** Create a copy to this new table, add the given table to the copy and return the copy **/
    JoinOrder addTableNew(uint32_t table) const;
    bool containsTable(uint32_t table) const;
    bool sameSet(const JoinOrder& other) const;

    uint32_t getArraySize() const;
    uint32_t getOrderedTables() const;
    const uint32_t* getTableOrder() const;

    friend std::ostream& operator<<(std::ostream& os, const JoinOrder& toPrint);
};

#endif /* JOINORDER_H_ */
