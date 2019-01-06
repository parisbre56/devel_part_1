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

class PartitionJob: public Callable<Tuple> {
public:
    PartitionJob();
    virtual ~PartitionJob();

    PartitionJob(const PartitionJob& toCopy) = delete;
    PartitionJob(PartitionJob&& toMove) = delete;
    PartitionJob& operator=(const PartitionJob& toCopy) = delete;
    PartitionJob& operator=(PartitionJob&& toMove) = delete;

    friend std::ostream& operator<<(std::ostream& os,
                                    const PartitionJob& toPrint);
};

#endif /* PARTITIONJOB_H_ */
