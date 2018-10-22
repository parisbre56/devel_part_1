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

    void setPayload(int32_t payload);
    void setKey(int32_t key);
public:
    Tuple() = delete;
    Tuple(int32_t key, int32_t payload);
    Tuple(const Tuple& toCopy);
    virtual ~Tuple();

    int32_t getKey() const;
    int32_t getPayload() const;

    std::string toString() const;
};

#endif /* TUPLE_H_ */
