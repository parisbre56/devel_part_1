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

unsigned char JoinRelation::sameTableAs(const JoinRelation& toCompare) const {
    if (leftNum == toCompare.leftNum && rightNum == toCompare.rightNum) {
        return 1;
    }
    if (leftNum == toCompare.rightNum && rightNum == toCompare.leftNum) {
        return 2;
    }
    return 0;
}

unsigned char JoinRelation::sameJoinAs(const JoinRelation& toCompare,
                                       const ResultContainer* const * const resultContainers) const {
    const ResultContainer* const leftResult = resultContainers[leftNum];
    const ResultContainer* const rightResult = resultContainers[rightNum];
    const ResultContainer* const toCompareLeftResult = resultContainers[toCompare.leftNum];
    const ResultContainer* const toCompareRightResult = resultContainers[toCompare.rightNum];
    if (leftResult != nullptr) {
        if (leftResult == toCompareLeftResult) {
            if (rightResult != nullptr) {
                if (rightResult == toCompareRightResult) {
                    return 1;
                }
            }
            else {
                if (rightNum == toCompare.rightNum) {
                    return 1;
                }
            }
        }
        else if (leftResult == toCompareRightResult) {
            if (rightResult != nullptr) {
                if (rightResult == toCompareLeftResult) {
                    return 2;
                }
            }
            else {
                if (rightNum == toCompare.leftNum) {
                    return 2;
                }
            }
        }
    }
    //Else if leftResult is null
    else {
        if (leftNum == toCompare.leftNum) {
            if (rightResult != nullptr) {
                if (rightResult == toCompareRightResult) {
                    return 1;
                }
            }
            else {
                if (rightNum == toCompare.rightNum) {
                    return 1;
                }
            }
        }
        else if (leftNum == toCompare.rightNum) {
            if (rightResult != nullptr) {
                if (rightResult == toCompareLeftResult) {
                    return 2;
                }
            }
            else {
                if (rightNum == toCompare.leftNum) {
                    return 2;
                }
            }
        }
    }
    return 0;
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

