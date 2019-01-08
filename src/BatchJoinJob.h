/*
 * BatchJoinJob.h
 *
 *  Created on: 8 Ιαν 2019
 *      Author: pibre
 */

#ifndef BATCHJOINJOB_H_
#define BATCHJOINJOB_H_
class BatchJoinJob;

#include <ostream>

#include "Callable.cpp"
#include "Join.h"
#include "JoinSumResult.h"

class BatchJoinJob: public Callable<JoinSumResult> {
protected:
    Join* join;
    JoinSumResult* result;

    void printSelf(std::ostream& os) const;
    JoinSumResult* getResultInternal() const;
    void runInternal();
public:
    /** Takes ownsership of join (deletes it at destruction) **/
    explicit BatchJoinJob(Join* join);
    BatchJoinJob(const BatchJoinJob& toCopy) = delete;
    BatchJoinJob(BatchJoinJob&& toMove) = delete;
    BatchJoinJob& operator=(const BatchJoinJob& toCopy) = delete;
    BatchJoinJob& operator=(BatchJoinJob&& toMove) = delete;
    virtual ~BatchJoinJob();

    const Join* getJoin() const;

    friend std::ostream& operator<<(std::ostream& os,
                                    const BatchJoinJob& toPrint);
};

#endif /* BATCHJOINJOB_H_ */
