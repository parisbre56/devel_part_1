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
    uint32_t sizeTableRows;
    size_t sizePayloads;
    uint64_t * tableRows;
    uint64_t * payloads;
public:
    Tuple() = delete;
    /** Create a Tuple with the values initialized to 0 **/
    Tuple(uint32_t sizeTableRows, size_t sizePayloads);
    /** Create a Tuple with the values of the given Tuple **/
    Tuple(const Tuple& toCopy);
    Tuple(Tuple&& toMove);

    /** Create a Tuple with the tableRows of the given Tuple
     * but an empty payloads table of the given size **/
    Tuple(const Tuple& toCopy, size_t sizePayloads);
    Tuple(Tuple&& toMove, size_t sizePayloads);
    /** Set the values of this Tuple to the values of the assigned Tuple **/
    Tuple& operator=(const Tuple& toCopy);
    Tuple& operator=(Tuple&& toMove);
    virtual ~Tuple();

    uint32_t Tuple::getSizeTableRows() const;
    uint64_t Tuple::getTableRow(uint32_t col) const;
    void Tuple::setTableRow(uint32_t col, uint64_t rowNum);

    size_t Tuple::getSizePayloads() const;
    uint64_t Tuple::getPayload(size_t col) const;
    void Tuple::setPayload(size_t col, uint64_t value);

    friend std::ostream& operator<<(std::ostream& os, const Tuple& toPrint);
};

#endif /* TUPLE_H_ */
