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

//1MB divided by size of Tuple gives us the number of Tuple we can store
#define RESULT_H_BLOCK_SIZE (1024*1024)/sizeof(Tuple)

class Result {
protected:
    Tuple* tuples;
    uint32_t numTuples;
    Result* next;

    Result* getLastSegment();
    void copyValuesInternal(const Result& toCopy);
    void moveValuesInternal(Result& toMove);

public:
    /** Creates a new empty Result **/
    Result();
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
    uint32_t getNumTuples() const;

    /** Add a copy of the Tuple to the array of tuples,
     * adding another segment automatically if necessary **/
    void addTuple(Tuple& toAdd);

    friend std::ostream& operator<<(std::ostream& os, const Result& toPrint);
};

#endif /* RESULT_H_ */
