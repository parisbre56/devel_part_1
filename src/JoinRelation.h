/*
 * JoinRelation.h
 *
 *  Created on: 25 Νοε 2018
 *      Author: parisbre56
 */

#ifndef JOINRELATION_H_
#define JOINRELATION_H_
class JoinRelation;

#include "Tuple.h"

#include <iostream>

class JoinRelation {
    const uint32_t leftNum;
    const size_t leftCol;
    const uint32_t rightNum;
    const size_t rightCol;
public:
    JoinRelation() = delete;
    JoinRelation(uint32_t leftNum,
                 size_t leftCol,
                 uint32_t rightNum,
                 size_t rightCol);
    JoinRelation(const JoinRelation& toCopy);
    JoinRelation& operator=(const JoinRelation& toCopy) = delete;
    virtual ~JoinRelation();

    bool sameTableAs(const JoinRelation& toCompare) const;

    uint32_t getLeftNum() const;
    size_t getLeftCol() const;
    uint32_t getRightNum() const;
    size_t getRightCol() const;

    friend std::ostream& operator<<(std::ostream& os,
                                    const JoinRelation& toPrint);
};

#endif /* JOINRELATION_H_ */
