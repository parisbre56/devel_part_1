/*
 * LoadRelationTableSegmentJob.h
 *
 *  Created on: 9 Ιαν 2019
 *      Author: pibre
 */

#ifndef LOADRELATIONRESULTJOB_H_
#define LOADRELATIONRESULTJOB_H_
class LoadRelationResultJob;

#include "Callable.h"
#include "Executor.h"
#include "Filter.h"
#include "Result.h"
#include "Relation.h"

class LoadRelationResultJob: public Callable<void> {
protected:
    Executor& executor;
    const Result& resultToLoad;
    Relation& relationToFill;
    uint64_t segmentStart;
    const size_t sizePayloads;
    const uint64_t * const * const payloadCols;
    const uint32_t * const payloadTables;
    LoadRelationResultJob* const child;

    LoadRelationResultJob* const createChild(Executor& executor,
                                             const Result& resultToLoad,
                                             Relation& relationToFill,
                                             uint64_t segmentStart,
                                             const size_t sizePayloads,
                                             const uint64_t * const * const payloadCols,
                                             const uint32_t * const payloadTables);
    void printSelf(std::ostream& os) const;
    void * getResultInternal();
    void runInternal();
public:
    LoadRelationResultJob(Executor& executor,
                          const Result& resultToLoad,
                          Relation& relationToFill,
                          uint64_t segmentStart,
                          const size_t sizePayloads,
                          const uint64_t * const * const payloadCols,
                          const uint32_t * const payloadTables);
    LoadRelationResultJob(const LoadRelationResultJob& toCopy) = delete;
    LoadRelationResultJob(LoadRelationResultJob&& toMove) = delete;
    LoadRelationResultJob& operator=(const LoadRelationResultJob& toCopy) = delete;
    LoadRelationResultJob& operator=(LoadRelationResultJob&& toMove) = delete;
    virtual ~LoadRelationResultJob();

    bool hasChild() const;

    friend std::ostream& operator<<(std::ostream& os,
                                    const LoadRelationResultJob& toPrint);
};

#endif /* LOADRELATIONRESULTJOB_H_ */
