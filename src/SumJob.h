/*
 * SumJob.h
 *
 *  Created on: 8 Ιαν 2019
 *      Author: pibre
 */

#ifndef SUMJOB_H_
#define SUMJOB_H_
class SumJob;

#include <ostream>

#include "Callable.h"
#include "Executor.h"
#include "Result.h"
#include "Table.h"

class SumJob: public Callable<const uint64_t> {
protected:
    Executor& executor;
    const Result& startResult;
    const uint32_t tableNum;
    const uint64_t* const tableColumn;

    SumJob* child;
    uint64_t result;

    SumJob* generateChild(Executor& executor,
                          const Result& startResult,
                          const uint32_t tableNum,
                          const uint64_t* const tableColumn);
    void printSelf(std::ostream& os) const;
    const uint64_t* getResultInternal();
    void runInternal();
public:
    SumJob(Executor& executor,
           const Result& startResult,
           const uint32_t tableNum,
           const uint64_t* const tableColumn);
    SumJob(const SumJob& toCopy) = delete;
    SumJob(SumJob&& toMove) = delete;
    SumJob& operator=(const SumJob& toCopy) = delete;
    SumJob& operator=(SumJob&& toMove) = delete;
    virtual ~SumJob();

    friend std::ostream& operator<<(std::ostream& os, const SumJob& toPrint);
};

#endif /* SUMJOB_H_ */
