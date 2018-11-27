/*
 * JoinRelation.cpp
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#include "JoinRelation.h"

using namespace std;

JoinRelation::JoinRelation(uint32_t leftNum,
                           size_t leftCol,
                           uint32_t rightNum,
                           size_t rightCol) :
        leftNum(leftNum),
        leftCol(leftCol),
        rightNum(rightNum),
        rightCol(rightCol) {

}
JoinRelation::JoinRelation(const JoinRelation& toCopy) :
        JoinRelation(toCopy.leftNum,
                     toCopy.leftCol,
                     toCopy.rightNum,
                     toCopy.rightCol) {

}
JoinRelation::~JoinRelation() {
    //Do nothing
}

uint32_t JoinRelation::getLeftNum() const {
    return leftNum;

}
size_t JoinRelation::getLeftCol() const {
    return leftCol;
}
uint32_t JoinRelation::getRightNum() const {
    return rightNum;
}
size_t JoinRelation::getRightCol() const {
    return rightCol;
}

ostream& operator<<(ostream& os, const JoinRelation& toPrint) {
    os << "[JoinRelation leftNum="
       << toPrint.leftNum
       << ", leftCol="
       << toPrint.leftCol
       << ", rightNum="
       << toPrint.rightNum
       << ", rightCol="
       << toPrint.rightCol
       << "]";
    return os;
}

