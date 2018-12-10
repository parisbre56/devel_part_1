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

#include "ResultContainer.h"

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

    /** Return 0 for false, 1 for right order, 2 for inverse order (right
     * matches cLeft, left matches cRight) **/
    unsigned char sameTableAs(const JoinRelation& toCompare) const;
    /** Similar to sameTableAs but also checks join results for each table to
     * determine if the two relations use the same join results. **/
    unsigned char sameJoinAs(const JoinRelation& toCompare,
                             const ResultContainer* const * const resultContainers) const;

    uint32_t getLeftNum() const;
    size_t getLeftCol() const;
    uint32_t getRightNum() const;
    size_t getRightCol() const;

    friend std::ostream& operator<<(std::ostream& os,
                                    const JoinRelation& toPrint);
};

#endif /* JOINRELATION_H_ */
