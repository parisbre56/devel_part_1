/*
 * Tuple.cpp
 *
 *  Created on: 21 ��� 2018
 *      Author: parisbre56
 */

#include "Tuple.h"

#include <sstream>
#include <cstring>

using namespace std;

Tuple::Tuple(uint32_t sizeTableRows, size_t sizePayloads) :
        sizeTableRows(sizeTableRows),
        sizePayloads(sizePayloads),
        tableRows(new uint64_t[sizeTableRows] { }),
        payloads(new uint64_t[sizePayloads] { }) {
}

Tuple::Tuple(const Tuple& toCopy) :
        sizeTableRows(toCopy.sizeTableRows),
        sizePayloads(toCopy.sizePayloads),
        tableRows(new uint64_t[toCopy.sizeTableRows]),
        payloads(new uint64_t[toCopy.sizePayloads]) {
    memcpy(tableRows, toCopy.tableRows, toCopy.sizeTableRows);
    memcpy(payloads, toCopy.payloads, toCopy.sizePayloads);
}
Tuple::Tuple(Tuple&& toMove) :
        sizeTableRows(toMove.sizeTableRows),
        sizePayloads(toMove.sizePayloads),
        tableRows(toMove.tableRows),
        payloads(toMove.payloads) {
    toMove.tableRows = nullptr;
    toMove.payloads = nullptr;
}

Tuple& Tuple::operator=(const Tuple& toCopy) {
    if (sizeTableRows < toCopy.sizeTableRows) {
        delete[] tableRows;
        tableRows = new uint64_t[toCopy.sizeTableRows];
    }
    if (sizePayloads < toCopy.sizePayloads) {
        delete[] payloads;
        payloads = new uint64_t[toCopy.sizePayloads];
    }
    sizeTableRows = toCopy.sizeTableRows;
    sizePayloads = toCopy.sizePayloads;
    memcpy(tableRows, toCopy.tableRows, toCopy.sizeTableRows);
    memcpy(payloads, toCopy.payloads, toCopy.sizePayloads);
}

Tuple& Tuple::operator=(Tuple&& toMove) {
    delete[] tableRows;
    delete[] payloads;
    tableRows = toMove.tableRows;
    payloads = toMove.payloads;
    toMove.tableRows = nullptr;
    toMove.payloads = nullptr;
    sizeTableRows = toMove.sizeTableRows;
    sizePayloads = toMove.sizePayloads;
}

Tuple::~Tuple() {
    if (tableRows != nullptr) {
        delete[] tableRows
    }
    if (payloads != nullptr) {
        delete[] payloads;
    }
}

uint32_t Tuple::getSizeTableRows() const {
    return sizeTableRows;
}

uint64_t Tuple::getTableRow(uint32_t col) const {
    return tableRows[col];
}
void Tuple::setTableRow(uint32_t col, uint64_t rowNum) {
    tableRows[col] = rowNum;
}

size_t Tuple::getSizePayloads() const {
    return sizePayloads;
}

uint64_t Tuple::getPayload(size_t col) const {
    return payloads[col];
}

void Tuple::setPayload(size_t col, uint64_t value) {
    payloads[col] = value;
}

std::ostream& operator<<(std::ostream& os, const Tuple& toPrint) {
    os << "[Tuple sizeTableRows="
       << toPrint.sizeTableRows
       << ", sizePayloads="
       << toPrint.sizePayloads
       << ", payloads=";
    if (toPrint.payloads == nullptr) {
        os << "null";
    }
    else {
        os << "[";
        for (uint32_t i = 0; i < toPrint.sizePayloads; ++i) {
            if (i != 0) {
                os << ", ";
            }
            os << toPrint.payloads[i];
        }
        os << "]";
    }
    os << ", tableRows=";
    if (toPrint.tableRows == nullptr) {
        os << "null";
    }
    else {
        os << "[";
        for (uint32_t i = 0; i < toPrint.sizeTableRows; ++i) {
            if (i != 0) {
                os << ", ";
            }
            os << toPrint.tableRows[i];
        }
        os << "]";
    }
    os << "]";
    return os;
}
