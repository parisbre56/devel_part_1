/*
 * Tuple.h
 *
 *  Created on: 21 Ïêô 2018
 *      Author: parisbre56
 */

#ifndef TUPLE_H_
#define TUPLE_H_

#include <cstdint>
#include <string>

class Tuple {
private:
    int32_t key;
    int32_t payload;
public:
    Tuple() = delete;
    Tuple(int32_t key, int32_t payload);
    Tuple(const Tuple& toCopy);
    virtual ~Tuple();

    int32_t getKey();
    void setKey(int32_t key);

    int32_t getPayload();
    void setPayload(int32_t payload);

    std::string toString();
};

#endif /* TUPLE_H_ */
