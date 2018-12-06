/*
 * ResultContainer.h
 *
 *  Created on: 29 Οκτ 2018
 *      Author: pibre
 */

#ifndef RESULTCONTAINER_H_
#define RESULTCONTAINER_H_

#include <string>

#include <cstdint>

#include "Result.h"
#include "Relation.h"

class ResultContainer {
protected:
    Result* start;
    Result* end;

    uint64_t resultCount;

    bool* usedRows;
    bool manageUsedRows;

    uint32_t sizeTableRows;
    size_t sizePayloads;
public:
    ResultContainer() = delete;
    ResultContainer(uint64_t blockSize,
                    uint32_t sizeTableRows,
                    size_t sizePayloads,
                    bool* usedRows = nullptr);
    ResultContainer(const ResultContainer& toCopy);
    ResultContainer(ResultContainer&& toMove);
    ResultContainer& operator=(const ResultContainer& toCopy);
    ResultContainer& operator=(ResultContainer&& toMove);
    virtual ~ResultContainer();

    void addTuple(Tuple& toAdd);
    void addTuple(Tuple&& toAdd);
    /** Reset the container without releasing storage **/
    void reset();
    uint64_t getResultCount() const;
    /** Load to the given relation the results contained within.
     * {payloadTables} is an array of size {sizePayloads} that
     * contains numbers < {sizeTableRows} that tells from which
     * table the values will be loaded.
     * {payloadCols} is the columns of the table from which the values will be loaded. **/
    void loadToRelation(Relation& rel,
                        const size_t sizePayloads,
                        const uint32_t * const payloadTables,
                        const uint64_t * const * const payloadCols) const;

    friend std::ostream& operator<<(std::ostream& os,
                                    const ResultContainer& toPrint);
};

#endif /* RESULTCONTAINER_H_ */
