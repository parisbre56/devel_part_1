/*
 * SetMap.cpp
 *
 *  Created on: 2 Ιαν 2019
 *      Author: parisbre56
 */

#include "JoinOrderContainer.h"

#include <cstring>

using namespace std;

JoinOrderContainer::JoinOrderContainer() :
        JoinOrderContainer(JOINORDERCONTAINER_H_DEFAULT_SIZE) {

}

JoinOrderContainer::JoinOrderContainer(uint32_t size) :
        size(size),
        used(0),
        joinOrders(new JoinOrder*[size]),
        stats(new MultipleColumnStats*[size]) {

}

JoinOrderContainer::~JoinOrderContainer() {
    if (joinOrders != nullptr) {
        for (uint32_t i = 0; i < used; ++i) {
            delete joinOrders[i];
        }
        delete[] joinOrders;
    }
}

/** Returns size for not found **/
uint32_t JoinOrderContainer::getIndexForSet(const JoinOrder& asSet) {
    for (uint32_t i = 0; i < used; ++i) {
        if (asSet.sameSet(*(joinOrders[i]))) {
            return i;
        }
    }
    return size;
}

void JoinOrderContainer::increaseSize() {
    uint32_t oldSize = size;
    size += JOINORDERCONTAINER_H_DEFAULT_SIZE_INCREASE;

    const JoinOrder** oldJoinOrders = joinOrders;
    joinOrders = new JoinOrder[size];
    memcpy(joinOrders, oldJoinOrders, oldSize * sizeof(JoinOrder*));
    delete[] oldJoinOrders;

    const MultipleColumnStats** oldStats = stats;
    stats = new MultipleColumnStats[size];
    memcpy(stats, oldStats, oldSize * sizeof(MultipleColumnStats*));
    delete[] oldStats;
}

/** True if added, false otherwise **/
bool JoinOrderContainer::addIfBetter(const JoinOrder& toAdd,
                                     const MultipleColumnStats& stat) {
    if (used == size) {
        increaseSize();
    }
    uint32_t index = getIndexForSet(toAdd);
    //Does not already exist
    if (index == size) {
        joinOrders[used] = new JoinOrder(toAdd);
        stats[used] = new MultipleColumnStats(stat);
        used++;
        return true;
    }
    //Else, it already exists. Check if the new one is better
    if (stats[index]->getColumnStats()->getTotalRows()
        > stat.getColumnStats()->getTotalRows()) {
        *(joinOrders[index]) = toAdd;
        *(stats[index]) = stat;
        return true;
    }
    //Else, if the old one is better
    return false;
}

bool JoinOrderContainer::addIfBetterMove(const JoinOrder&& toAdd,
                                         const MultipleColumnStats&& stat) {
    if (used == size) {
        increaseSize();
    }
    uint32_t index = getIndexForSet(toAdd);
    //Does not already exist
    if (index == size) {
        joinOrders[used] = new JoinOrder(move(toAdd));
        stats[used] = new MultipleColumnStats(move(stat));
        used++;
        return true;
    }
    //Else, it already exists. Check if the new one is better
    if (stats[index]->getColumnStats()->getTotalRows()
        > stat.getColumnStats()->getTotalRows()) {
        *(joinOrders[index]) = move(toAdd);
        *(stats[index]) = move(stat);
        return true;
    }
    //Else, if the old one is better
    return false;
}

const JoinOrder * JoinOrderContainer::getOrderForSet(const JoinOrder& asSet) const {
    uint32_t index = getIndexForSet(asSet);
    if (index == size) {
        return nullptr;
    }
    return joinOrders[index];
}

const MultipleColumnStats * JoinOrderContainer::getStatForSet(const JoinOrder& asSet) const {
    uint32_t index = getIndexForSet(asSet);
    if (index == size) {
        return nullptr;
    }
    return stats[index];
}

const JoinOrder * const * JoinOrderContainer::getJoinOrders() const {
    return joinOrders;
}

const MultipleColumnStats * const * JoinOrderContainer::getStats() const {
    return stats;
}

uint32_t JoinOrderContainer::getSize() const {
    return size;
}
uint32_t JoinOrderContainer::getUsed() const {
    return used;
}

ostream& operator<<(ostream& os, const JoinOrderContainer& toPrint) {
    os << "[JoinOrderContainer size="
       << toPrint.size
       << ", used="
       << toPrint.used
       << ", joinOrders=";
    if (toPrint.joinOrders == nullptr) {
        os << "null";
    }
    else {
        os << "[";
        for (uint32_t i = 0; i < toPrint.used; ++i) {
            os << "\n\t" << toPrint.joinOrders[i];
        }
        os << "]";
    }
    os << ", stats=";
    if (toPrint.stats == nullptr) {
        os << "null";
    }
    else {
        os << "[";
        for (uint32_t i = 0; i < toPrint.used; ++i) {
            os << "\n\t" << toPrint.stats[i];
        }
        os << "]";
    }
    return os;
}
