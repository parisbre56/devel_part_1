/*
 * PartitionJob.h
 *
 *  Created on: 7 Ιαν 2019
 *      Author: parisbre56
 */

#ifndef PARTITIONJOB_H_
#define PARTITIONJOB_H_
class PartitionJob;

#include <ostream>

#include "Callable.h"
#include "Tuple.h"
#include "HistogramJob.h"

class PartitionJob: public Callable<void> {
protected:
    const HistogramJob& histogramJob;
    const Tuple* * const outputTuples;
    uint64_t* segmentPSum;

    void printSelf(std::ostream& os) const;
    void * getResultInternal() const;
    void runInternal();
public:
    PartitionJob(const HistogramJob& histogramJob,
                 const uint64_t * const inSegmentPSum,
                 const Tuple* * const outputTuples);
    PartitionJob(const PartitionJob& toCopy) = delete;
    PartitionJob(PartitionJob&& toMove) = delete;
    PartitionJob& operator=(const PartitionJob& toCopy) = delete;
    PartitionJob& operator=(PartitionJob&& toMove) = delete;
    virtual ~PartitionJob();

    friend std::ostream& operator<<(std::ostream& os,
                                    const PartitionJob& toPrint);
};

#endif /* PARTITIONJOB_H_ */
