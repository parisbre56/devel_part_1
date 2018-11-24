/*
 * Metadata.cpp
 *
 *  Created on: 23 Νοε 2018
 *      Author: parisbre56
 */

#include "Metadata.h"

Metadata::Metadata(TableLoader& tableLoader) :
        tableLoader(tableLoader) {
    // TODO Auto-generated constructor stub

}

Metadata::~Metadata() {
    // TODO Auto-generated destructor stub
}

void Metadata::addTable(uint32_t table) {
}
void Metadata::addTableRelationship(uint32_t tableA,
                                    size_t columnA,
                                    uint32_t tableB,
                                    size_t columnB) {
}
void Metadata::addTableFilter(uint32_t table,
                              size_t column,
                              uint64_t filterNumber,
                              char type) {
}
void Metadata::addSumColumn(uint32_t table, size_t column) {
}

void Metadata::startJoin() {
}
void Metadata::endJoin() {
}
void Metadata::endBatch() {
}
