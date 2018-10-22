/*
 * Relation.h
 *
 *  Created on: 21 ��� 2018
 *      Author: parisbre56
 */

#ifndef RELATION_H_
#define RELATION_H_

#include <cstdint>
#include <string>

#include "Tuple.h"

class Relation {
protected:
    const Tuple** tuples;
    uint32_t num_tuples;

    uint32_t array_size;
public:
    /** Creates a new Relation with no tuples stored
     * and a predefined number of empty spaces in the
     * table. **/
    Relation();
    /** Creates a new Relation by copying the given number
     * of tuples from the given table. **/
    Relation(uint32_t tuples_to_copy, const Tuple * const * const tuples);
    /** Copy constructor, copies the filled part of the
     * underlying array from another Relation. **/
    Relation(const Relation&);
    virtual ~Relation();

    uint32_t getNumTuples() const;

    /** Add a copy of the Tuple to the array of tuples,
     * incrementing the size of the underlying array
     * automatically if necessary.
     *
     * The Relation will delete the created Tuple when it
     * is deleted. **/
    void addTuple(Tuple& tuple);
    /** Get the tuple at the given index. **/
    const Tuple& getTuple(uint32_t index) const;

    std::string toString() const;
};

#endif /* RELATION_H_ */
