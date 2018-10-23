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
#define BLOCK_SIZE (1024*1024)/sizeof(Tuple)

class Result {
protected:
    Tuple tuples[BLOCK_SIZE];
    uint32_t numTuples;
    Result* next;

    Result* getLastSegment();
    void copyValuesInternal(const Result& toCopy);
public:
    /** Creates a new Result with no tuples stored
     * and a predefined number of empty spaces in the
     * table. **/
    Result();
    /** Copy constructor, copies the filled part of the
     * underlying array from another Result. **/
    Result(const Result& toCopy);
    Result& operator=(const Result& toCopy);
    virtual ~Result();

    /** The number of tuples in this segment **/
    uint32_t getNumTuples() const;

    /** Add a copy of the Tuple to the array of tuples,
     * adding another segment automatically if necessary **/
    void addTuple(Tuple& toAdd);

    std::string toString() const;
};

#endif /* RESULT_H_ */
