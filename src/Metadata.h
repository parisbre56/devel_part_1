/*
 * Metadata.h
 *
 *  Created on: 23 Νοε 2018
 *      Author: parisbre56
 */

#ifndef METADATA_H_
#define METADATA_H_

#include "TableLoader.h"

class Metadata {
protected:
    TableLoader& tableLoader;
public:
    Metadata() = delete;
    Metadata(TableLoader& tableLoader);
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
};

#endif /* METADATA_H_ */
