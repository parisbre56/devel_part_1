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

    void increaseSize();
public:
    JoinOrderContainer();
    explicit JoinOrderContainer(uint32_t size);
    JoinOrderContainer(const JoinOrderContainer& toCopy) = delete;
    JoinOrderContainer(JoinOrderContainer&& toMove) = delete;
    JoinOrderContainer& operator=(const JoinOrderContainer& toCopy) = delete;
    JoinOrderContainer& operator=(JoinOrderContainer&& toMove) = delete;
    virtual ~JoinOrderContainer();

    /** Returns size for not found **/
    uint32_t getIndexForSet(const JoinOrder& asSet) const;
    /** True if added, false otherwise **/
    bool addIfBetter(const JoinOrder& toAdd, const MultipleColumnStats& stat);
    /** True if added, false otherwise **/
    bool addIfBetterMove(JoinOrder&& toAdd, MultipleColumnStats&& stat);
    /** Move from the given container the entry at the given index to this container if it is better than the existing for that subset.
     * If it is moved, the entry at the given container is left in an unusable state.
     * True if added, false otherwise **/
    bool stealEntry(JoinOrderContainer& stealFrom, uint32_t index);
    /** Return nullptr for not found **/
    const JoinOrder * getOrderForSet(const JoinOrder& asSet) const;
    /** Return nullptr for not found **/
    const MultipleColumnStats * getStatForSet(const JoinOrder& asSet) const;
    const JoinOrder * const * getJoinOrders() const;
    const MultipleColumnStats * const * getStats() const;
    uint32_t getSize() const;
    uint32_t getUsed() const;

    friend std::ostream& operator<<(std::ostream& os, const JoinOrderContainer& toPrint);
};

#endif /* JOINORDERCONTAINER_H_ */
