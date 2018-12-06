/*
 * Result.h
 *
 *  Created on: 21 ��� 2018
 *      Author: parisbre56
 */

#ifndef RESULT_H_
#define RESULT_H_

#include <cstdint>
#include <string>

#include "Tuple.h"

//TODO dynamically compute?
//1MB divided by size of Tuple gives us the number of Tuple we can store
//#define RESULT_H_BLOCK_SIZE (1024*1024)/sizeof(Tuple) //TODO this is wrong

class Result {
protected:
    Relation* relation;
    Result* next;

    bool* usedRows;

    uint32_t sizeTableRows;
    size_t sizePayloads;
public:
    Result() = delete;
    /** Creates a new empty Result **/
    Result(uint64_t blockSize,
           int32_t sizeTableRows,
           size_t sizePayloads,
           bool* usedRows);
    /** Copy constructor, copies the contents of the given Result
     * to the new Result. **/
    Result(const Result& toCopy);
    /** Move the data from the given Result to a new one. The old
     * Result is left empty and unusable, it can only be deleted. **/
    Result(Result&& toMove);
    /** Copy assignment operator, copies the content of the given Result
     * to the new Result. If this has more segments than toCopy, then those
     * segments will be left empty but will not be deleted. **/
    Result& operator=(const Result& toCopy);
    /** Move assignment operator, move the data from the given Result
     * to this one. If this result contains data it will be deleted.
     * The old result is left unusable and can only be deleted. **/
    Result& operator=(Result&& toMove);
    virtual ~Result();

    /** The number of tuples in this segment **/
    const Relation& getRelation() const;

    /** Returns the last segment for this chain of results. **/
    Result* getLastSegment();
    /** Return the first non-full segment. Or the last segment if none exist. **/
    Result* getFirstNonFullSegment();

    /** Add a copy of the Tuple to the array of tuples,
     * adding another segment automatically if necessary.
     * Returns the segment into which the tuple was inserted
     * if it was inserted into a segment other than this
     * or null if it was inserted to this segment. **/
    Result* addTuple(Tuple& toAdd);
    Result* addTuple(Tuple&& toAdd);

    void reset();

    friend std::ostream& operator<<(std::ostream& os, const Result& toPrint);
};

#endif /* RESULT_H_ */
