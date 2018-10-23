/*
 * Tuple.h
 *
 *  Created on: 21 ��� 2018
 *      Author: parisbre56
 */

#ifndef TUPLE_H_
#define TUPLE_H_

#include <cstdint>
#include <string>

class Tuple {
protected:
    int32_t key;
    int32_t payload;

public:
    Tuple();
    Tuple(int32_t key, int32_t payload);
    Tuple(const Tuple& toCopy);
    Tuple& operator=(const Tuple& toCopy);
    virtual ~Tuple();

    int32_t getKey() const;
    void setKey(int32_t key);
    int32_t getPayload() const;
    void setPayload(int32_t payload);

    std::string toString() const;
};

#endif /* TUPLE_H_ */
