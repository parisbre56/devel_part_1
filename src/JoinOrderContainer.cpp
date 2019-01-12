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
        stats(new MultipleColumnStats*[size]),
        rowSums(new double[size]) {

}

JoinOrderContainer::~JoinOrderContainer() {
    if (joinOrders != nullptr) {
        for (uint32_t i = 0; i < used; ++i) {
            delete joinOrders[i];
        }
        delete[] joinOrders;
    }
    if (stats != nullptr) {
        for (uint32_t i = 0; i < used; ++i) {
            delete stats[i];
        }
        delete[] stats;
    }
    if (rowSums != nullptr) {
        delete[] rowSums;
    }
}

/** Returns size for not found **/
uint32_t JoinOrderContainer::getIndexForSet(const JoinOrder& asSet) const {
    for (uint32_t i = 0; i < used; ++i) {
        if (asSet.sameSet(*(joinOrders[i]))) {
            return i;
        }
    }
    return size;
}

void JoinOrderContainer::increaseSize() {
    uint32_t oldSize = used;
    size += JOINORDERCONTAINER_H_DEFAULT_SIZE_INCREASE;

    JoinOrder** oldJoinOrders = joinOrders;
    joinOrders = new JoinOrder*[size];
    memcpy(joinOrders, oldJoinOrders, oldSize * sizeof(JoinOrder*));
    delete[] oldJoinOrders;

    MultipleColumnStats** oldStats = stats;
    stats = new MultipleColumnStats*[size];
    memcpy(stats, oldStats, oldSize * sizeof(MultipleColumnStats*));
    delete[] oldStats;

    double* oldSum = rowSums;
    rowSums = new double[size];
    memcpy(rowSums, oldSum, oldSize * sizeof(double));
}

/** True if added, false otherwise **/
bool JoinOrderContainer::addIfBetter(const JoinOrder& toAdd,
                                     const MultipleColumnStats& stat,
                                     const double rowSum) {
    uint32_t index = getIndexForSet(toAdd);
    //Does not already exist
    if (index == size) {
        if (used == size) {
            increaseSize();
        }
        joinOrders[used] = new JoinOrder(toAdd);
        stats[used] = new MultipleColumnStats(stat);
        rowSums[used] = rowSum;
        used++;
        return true;
    }
    //Else, it already exists. Check if the new one is better
    if (rowSums[index] > rowSum) {
        *(joinOrders[index]) = toAdd;
        *(stats[index]) = stat;
        rowSums[index] = rowSum;
        return true;
    }
    //Else, if the old one is better
    return false;
}

bool JoinOrderContainer::addIfBetterMove(JoinOrder&& toAdd,
                                         MultipleColumnStats&& stat,
                                         const double rowSum) {
    uint32_t index = getIndexForSet(toAdd);
    //Does not already exist
    if (index == size) {
        if (used == size) {
            increaseSize();
        }
        joinOrders[used] = new JoinOrder(move(toAdd));
        stats[used] = new MultipleColumnStats(move(stat));
        rowSums[used] = rowSum;
        used++;
        return true;
    }
    //Else, it already exists. Check if the new one is better
    if (rowSums[index] > rowSum) {
        *(joinOrders[index]) = move(toAdd);
        *(stats[index]) = move(stat);
        rowSums[index] = rowSum;
        return true;
    }
    //Else, if the old one is better
    return false;
}

bool JoinOrderContainer::stealEntry(JoinOrderContainer& stealFrom,
                                    uint32_t index) {
    if (index >= stealFrom.used) {
        throw runtime_error("index out of bounds [index="
                            + to_string(index)
                            + ", stealFrom.used="
                            + to_string(stealFrom.used)
                            + "]");
    }
    return addIfBetterMove(move(*(stealFrom.joinOrders[index])),
                           move(*(stealFrom.stats[index])),
                           stealFrom.rowSums[index]);
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

const double * JoinOrderContainer::getRowSums() const {
    return rowSums;
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
            os << "\n\t" << *(toPrint.joinOrders[i]);
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
            os << "\n\t" << *(toPrint.stats[i]);
        }
        os << "]";
    }
    os << ", rowSums=";
    if (toPrint.rowSums == nullptr) {
        os << "null";
    }
    else {
        os << "[";
        for (uint32_t i = 0; i < toPrint.used; ++i) {
            os << "\n\t" << toPrint.rowSums[i];
        }
        os << "]";
    }
    os << "]";
    return os;
}
