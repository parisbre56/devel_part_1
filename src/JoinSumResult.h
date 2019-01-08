/*
 * JoinSumResult.h
 *
 *  Created on: 27 Νοε 2018
 *      Author: parisbre56
 */

#ifndef JOINSUMRESULT_H_
#define JOINSUMRESULT_H_
class JoinSumResult;

#include <iostream>

class JoinSumResult {
protected:
    uint32_t numOfSums;
    uint64_t* sums;
    bool hasResults;
public:
    JoinSumResult() = delete;
    explicit JoinSumResult(uint32_t numOfSums);
    JoinSumResult(const JoinSumResult& toCopy) = delete;
    JoinSumResult(JoinSumResult&& toMove);
    JoinSumResult& operator=(const JoinSumResult& toCopy) = delete;
    JoinSumResult& operator=(JoinSumResult&& toMove);
    virtual ~JoinSumResult();

    uint32_t getNumOfSums() const;
    uint64_t getSum(const uint32_t sumNum) const;
    uint64_t addSum(const uint32_t sumNum, const uint64_t toAdd);
    bool getHasResults() const;
    void setHasResults();

    friend std::ostream& operator<<(std::ostream& os,
                                    const JoinSumResult& toPrint);
};

#endif /* JOINSUMRESULT_H_ */
