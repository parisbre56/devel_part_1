/*
 * SetMap.h
 *
 *  Created on: 2 Ιαν 2019
 *      Author: parisbre56
 */

#ifndef JOINORDERCONTAINER_H_
#define JOINORDERCONTAINER_H_
class JoinOrderContainer;

#define JOINORDERCONTAINER_H_DEFAULT_SIZE 100
#define JOINORDERCONTAINER_H_DEFAULT_SIZE_INCREASE 100

#include <ostream>

#include "JoinOrder.h"
#include "MultipleColumnStats.h"

class JoinOrderContainer {
protected:
    uint32_t size;
    uint32_t used;
    JoinOrder** joinOrders;
    MultipleColumnStats** stats;

    /** Returns size for not found **/
    uint32_t getIndexForSet(const JoinOrder& asSet);
    void increaseSize();
public:
    JoinOrderContainer();
    explicit JoinOrderContainer(uint32_t size);
    JoinOrderContainer(const JoinOrderContainer& toCopy) = delete;
    JoinOrderContainer(JoinOrderContainer&& toMove) = delete;
    JoinOrderContainer& operator=(const JoinOrderContainer& toCopy) = delete;
    JoinOrderContainer& operator=(JoinOrderContainer&& toMove) = delete;
    virtual ~JoinOrderContainer();

    /** True if added, false otherwise **/
    bool addIfBetter(const JoinOrder& toAdd, const MultipleColumnStats& stat);
    bool addIfBetterMove(const JoinOrder&& toAdd,
                         const MultipleColumnStats&& stat);
    const JoinOrder * getForSet(const JoinOrder& asSet) const;
    const JoinOrder * const * getJoinOrders() const;
    uint32_t getSize() const;
    uint32_t getUsed() const;

    friend std::ostream& operator<<(std::ostream& os, const JoinOrderContainer& toPrint);
};

#endif /* JOINORDERCONTAINER_H_ */
