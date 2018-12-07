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
    uint32_t sizeTableRows;
    size_t sizePayloads;

    uint64_t resultCount;

    bool* usedRows;
    bool manageUsedRows;

    Result* start;
    Result* end;
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
    const bool * getUsedRows() const;
    void setUsedRow(uint32_t col);
    /** Load to the given relation the results contained within.
     * {payloadTables} is an array of size {sizePayloads} that
     * contains numbers < {sizeTableRows} that tells from which
     * table the values will be loaded.
     * {payloadCols} is an array of size {sizePayloads} that has
     * the columns of the table from which the values will be loaded. **/
    Relation loadToRelation(const uint32_t payloadTable,
                            const size_t sizePayloads,
                            const uint64_t * const * const payloadCols) const;

    friend std::ostream& operator<<(std::ostream& os,
                                    const ResultContainer& toPrint);
};

#endif /* RESULTCONTAINER_H_ */
