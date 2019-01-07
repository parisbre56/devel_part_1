/*
 * JoinJob.h
 *
 *  Created on: 7 Ιαν 2019
 *      Author: pibre
 */

#ifndef JOINJOB_H_
#define JOINJOB_H_
class JoinJob;

#include "Callable.h"
#include "ResultContainer.h"
#include "HashTableJob.h"

class JoinJob: public Callable<ResultContainer> {
protected:
    HashTableJob& rHash;
    HashTableJob& sHash;
    const uint32_t bucket;
    const bool* const usedRows;
    ResultContainer* result;

    void printSelf(std::ostream& os) const;
    ResultContainer* getResultInternal() const;
    void runInternal();
public:
    JoinJob(HashTableJob& rHash,
            HashTableJob& sHash,
            const uint32_t bucket,
            const bool* usedRows);
    JoinJob(const JoinJob& toCopy) = delete;
    JoinJob(JoinJob&& toMove) = delete;
    JoinJob& operator=(const JoinJob& toCopy) = delete;
    JoinJob& operator=(JoinJob&& toMove) = delete;
    virtual ~JoinJob();

    friend std::ostream& operator<<(std::ostream& os, const JoinJob& toPrint);
};

#endif /* JOINJOB_H_ */
