/*
 * JoinSumResult.h
 *
 *  Created on: 27 Νοε 2018
 *      Author: parisbre56
 */

#ifndef JOINSUMRESULT_H_
#define JOINSUMRESULT_H_

#include <iostream>

class JoinSumResult {
protected:
    uint32_t numOfSums;
    uint64_t* sums;
public:
    JoinSumResult() = delete;
    JoinSumResult(uint32_t numOfSums);
    JoinSumResult(const JoinSumResult& toCopy) = delete;
    JoinSumResult(JoinSumResult&& toMove);
    JoinSumResult& operator=(const JoinSumResult& toCopy) = delete;
    JoinSumResult& operator=(JoinSumResult&& toMove);
    virtual ~JoinSumResult();

    uint32_t getNumOfSums();
    uint64_t getSum(uint32_t sumNum);
    uint64_t addSum(uint32_t sumNum, uint64_t toAdd);

    friend std::ostream& operator<<(std::ostream& os,
                                    const JoinSumResult& toPrint);
};

#endif /* JOINSUMRESULT_H_ */
