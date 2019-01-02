/*
 * SetMap.cpp
 *
 *  Created on: 2 Ιαν 2019
 *      Author: parisbre56
 */

#include "JoinOrderContainer.h"

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

    MultipleColumnStats** oldStats = stats;
}

/** True if added, false otherwise **/
bool JoinOrderContainer::addIfBetter(const JoinOrder& toAdd,
                                     const MultipleColumnStats& stat) {
    if (used == size) {
        increaseSize();
    }

}

bool JoinOrderContainer::addIfBetterMove(const JoinOrder&& toAdd,
                                         const MultipleColumnStats&& stat) {
    if (used == size) {
        increaseSize();
    }

}

const JoinOrder * JoinOrderContainer::getForSet(const JoinOrder& asSet) const {

}
const JoinOrder * const * JoinOrderContainer::getJoinOrders() const {

}
uint32_t JoinOrderContainer::getSize() const {

}
uint32_t JoinOrderContainer::getUsed() const {

}

ostream& operator<<(ostream& os, const JoinOrderContainer& toPrint) {

}
