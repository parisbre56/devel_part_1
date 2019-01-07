/*
 * Result.h
 *
 *  Created on: 21 ��� 2018
 *      Author: parisbre56
 */

#ifndef RESULT_H_
#define RESULT_H_
class Result;

#include <cstdint>
#include <string>

#include "Tuple.h"
#include "Relation.h"

class Result {
protected:
    uint32_t sizeTableRows;
    size_t sizePayloads;

    bool* usedRows;

    Relation* relation;
    Result* next;

    void getLastSegment(Result*& lastSegment, Result*& createdSegment);

public:
    Result() = delete;
    /** Creates a new empty Result. Does not manage usedRows **/
    Result(uint64_t blockSize,
           int32_t sizeTableRows,
           size_t sizePayloads,
           bool* usedRows);
    /** Copy constructor, copies the contents of the given Result
     * to the new Result. Need to explicitly provide usedRows since
     * we don't know who is the owner of that memory in relation to this
     * object. **/
    Result(const Result& toCopy) = delete;
    Result(const Result& toCopy, bool * usedRows);
    /** Move the data from the given Result to a new one. The old
     * Result is left empty and unusable, it can only be deleted. **/
    Result(Result&& toMove);
    /** Copy assignment operator, copies the content of the given Result
     * to the new Result. If this has more segments than toCopy, then those
     * segments will be left empty but will not be deleted. **/
    Result& operator=(const Result& toCopy) = delete;
    /** Move assignment operator, move the data from the given Result
     * to this one. If this result contains data it will be deleted.
     * The old result is left unusable and can only be deleted. **/
    Result& operator=(Result&& toMove) = delete;
    virtual ~Result();

    /** The tuples in this segment **/
    const Relation& getRelation() const;

    Result* getNext() const;
    void setNext(Result* next);
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
    /** Move a tuple **/
    Result* addTuple(Tuple&& toAdd);

    void reset();

    friend std::ostream& operator<<(std::ostream& os, const Result& toPrint);
};

#endif /* RESULT_H_ */
