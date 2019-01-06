/*
 * HistogramJob.h
 *
 *  Created on: 6 Ιαν 2019
 *      Author: parisbre56
 */

#ifndef HISTOGRAMJOB_H_
#define HISTOGRAMJOB_H_
class HistogramJob;

#include <ostream>

#include "Callable.h"
#include "Relation.h"
#include "HashFunction.h"

class HistogramJob: public Callable<uint64_t> {
protected:
    const Relation& relation;
    const HashFunction& hashFunction;
    const uint64_t segmentStartInclusive;
    const uint64_t segmentSize;

    uint64_t* result;

    void printSelf(std::ostream& os) const;
    uint64_t* getResultInternal() const;
    void runInternal();
public:
    HistogramJob(Relation& relation,
                 HashFunction& hashFunction,
                 uint64_t segmentStartInclusive,
                 uint64_t segmentSize);
    HistogramJob(const HistogramJob& toCopy) = delete;
    HistogramJob(HistogramJob&& toMove) = delete;
    HistogramJob& operator=(const HistogramJob& toCopy) = delete;
    HistogramJob& operator=(HistogramJob&& toMove) = delete;
    virtual ~HistogramJob();

    const HashFunction&& getHashFunction() const;
    const Relation&& getRelation() const;
    const uint64_t getSegmentSize() const;
    const uint64_t getSegmentStartInclusive() const;

    friend std::ostream& operator<<(std::ostream& os,
                                    const HistogramJob& toPrint);
};

#endif /* HISTOGRAMJOB_H_ */
