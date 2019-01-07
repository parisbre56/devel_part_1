/*
 * Metadata.h
 *
 *  Created on: 23 Νοε 2018
 *      Author: parisbre56
 */

#ifndef METADATA_H_
#define METADATA_H_
class Metadata;

#include "TableLoader.h"
#include "Join.h"
#include "Executor.h"

#define METADATA_H_THREAD_NUM 10
#define METADATA_H_QUEUE_SIZE 1000

class Metadata {
protected:
    const TableLoader& tableLoader;
    Join** batch;
    Join* activeJoin;
    const uint32_t arraySize;
    uint32_t joinsInBatch;
    Executor* executor;

    void resetBatch();

public:
    Metadata() = delete;
    Metadata(const TableLoader& tableLoader, uint32_t arraySize);
    Metadata(const Metadata& toCopy) = delete;
    Metadata(Metadata&& toMove) = delete;
    Metadata& operator=(const Metadata& toCopy) = delete;
    Metadata& operator=(Metadata&& toMove) = delete;
    virtual ~Metadata();

    void addTable(uint32_t table);
    void addTableRelationship(uint32_t tableA,
                              size_t columnA,
                              uint32_t tableB,
                              size_t columnB);
    void addTableFilter(uint32_t table,
                        size_t column,
                        uint64_t filterNumber,
                        char type);
    void addSumColumn(uint32_t table, size_t column);

    void startJoin();
    void endJoin();
    void endBatch();

    friend std::ostream& operator<<(std::ostream& os, const Metadata& toPrint);
};

#endif /* METADATA_H_ */
