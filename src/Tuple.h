/*
 * Tuple.h
 *
 *  Created on: 21 ��� 2018
 *      Author: parisbre56
 */

#ifndef TUPLE_H_
#define TUPLE_H_

#include <string>

#include <cstdint>

class Tuple {
protected:
    uint64_t key;
    uint64_t payload;

public:
    /** Create a Tuple with all values initialized to 0 **/
    Tuple();
    /** Create a Tuple with values initialized to the given values **/
    Tuple(uint64_t key, uint64_t payload);
    /** Create a Tuple with the values of the given Tuple **/
    Tuple(const Tuple& toCopy);
    /** Set the values of this Tuple to the values of the assigned Tuple **/
    Tuple& operator=(const Tuple& toCopy);
    virtual ~Tuple();

    uint64_t getKey() const;
    void setKey(uint64_t key);
    uint64_t getPayload() const;
    void setPayload(uint64_t payload);

    friend std::ostream& operator<<(std::ostream& os, const Tuple& toPrint);
};

#endif /* TUPLE_H_ */
