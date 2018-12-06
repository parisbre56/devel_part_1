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
public:
    ResultContainer();
    ResultContainer(const ResultContainer& toCopy);
    ResultContainer(ResultContainer&& toMove);
    ResultContainer& operator=(const ResultContainer& toCopy);
    ResultContainer& operator=(ResultContainer&& toMove);
    virtual ~ResultContainer();

    void addTuple(Tuple& toAdd);
    /** Reset the container without releasing storage **/
    void reset();
    uint64_t getResultCount() const;
    /** Load to given relation the results contained within.
     * currCol is a pointer to the column to use.
     * useKey is set to true if the table was stored on the key part of the result or false if it was stored on the payload part. **/
    void loadToRelation(Relation& rel,
                        const uint64_t * const currCol,
                        const bool useKey) const;

    friend std::ostream& operator<<(std::ostream& os,
                                    const ResultContainer& toPrint);
};

#endif /* RESULTCONTAINER_H_ */
