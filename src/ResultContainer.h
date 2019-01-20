/*
 * ResultContainer.h
 *
 *  Created on: 29 Οκτ 2018
 *      Author: pibre
 */

#ifndef RESULTCONTAINER_H_
#define RESULTCONTAINER_H_
class ResultContainer;

#include <string>

#include <cstdint>

#include "Result.h"
#include "Relation.h"
#include "Executor.h"

#define RESULTCONTAINER_H_L2_CACHE_SIZE 262144

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
                    const bool* usedRows = nullptr,
                    Result* toReuse = nullptr);
    ResultContainer(const ResultContainer& toCopy);
    ResultContainer(ResultContainer&& toMove);
    ResultContainer& operator=(const ResultContainer& toCopy);
    ResultContainer& operator=(ResultContainer&& toMove);
    virtual ~ResultContainer();

    const Result* getFirstResultBlock() const;

    void addTuple(Tuple& toAdd);
    void addTuple(Tuple&& toAdd);
    /** Reset the container without releasing storage **/
    void reset();
    void relinquish();
    uint64_t getResultCount() const;
    const bool * getUsedRows() const;
    void setUsedRow(uint32_t col);
    /** Load to the given relation the results contained within.
     * {payloadTables} is an array of size {sizePayloads} that
     * contains numbers < {sizeTableRows} that tells from which
     * table the values will be loaded.
     * {payloadCols} is an array of size {sizePayloads} that has
     * the columns of the table from which the values will be loaded. **/
    Relation loadToRelation(Executor& executor,
                            const size_t sizePayloads,
                            const uint64_t * const * const payloadCols,
                            const uint32_t * const payloadTables) const;
    void mergeResult(ResultContainer&& toMerge);

    friend std::ostream& operator<<(std::ostream& os,
                                    const ResultContainer& toPrint);
};

#endif /* RESULTCONTAINER_H_ */
