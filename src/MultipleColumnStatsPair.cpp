/*
 * MultipleColumnStatsPair.cpp
 *
 *  Created on: 23 Δεκ 2018
 *      Author: parisbre56
 */

#include "MultipleColumnStatsPair.h"

using namespace std;

MultipleColumnStatsPair::MultipleColumnStatsPair(const MultipleColumnStats& left,
                                                 const MultipleColumnStats& right) :
        left(new MultipleColumnStats(left)),
        right(new MultipleColumnStats(right)) {

}

MultipleColumnStatsPair::MultipleColumnStatsPair(MultipleColumnStatsPair&& toMove) :
        left(toMove.left), right(toMove.right) {
    toMove.left = nullptr;
    toMove.right = nullptr;
}

MultipleColumnStatsPair::~MultipleColumnStatsPair() {
    if (left != nullptr) {
        delete left;
    }
    if (right != nullptr) {
        delete right;
    }
}

MultipleColumnStats& MultipleColumnStatsPair::getLeft() {
    return *left;
}

MultipleColumnStats& MultipleColumnStatsPair::getRight() {
    return *right;
}

const MultipleColumnStats& MultipleColumnStatsPair::getLeftConst() const {
    return *left;
}

const MultipleColumnStats& MultipleColumnStatsPair::getRightConst() const {
    return *right;
}

ostream& operator<<(ostream& os,
                           const MultipleColumnStatsPair& toPrint) {
    os << "[MultipleColumnStatsPair left="
       << toPrint.left
       << ", right="
       << toPrint.right
       << "]";
    return os;
}
