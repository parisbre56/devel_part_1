/*
 * Metadata.cpp
 *
 *  Created on: 23 Νοε 2018
 *      Author: parisbre56
 */

#include "Metadata.h"

#include <stdexcept>

using namespace std;

Metadata::Metadata(const TableLoader& tableLoader, uint32_t arraySize) :
        tableLoader(tableLoader),
        batch(new BatchJoinJob*[arraySize]),
        activeJoin(nullptr),
        arraySize(arraySize),
        joinsInBatch(0),
        hashExecutor(new Executor(METADATA_H_THREAD_NUM_HASH,
        METADATA_H_QUEUE_SIZE,
                                  "Hash")),
        joinExecutor(new Executor(METADATA_H_THREAD_NUM_JOIN,
        METADATA_H_QUEUE_SIZE,
                                  "Join")),
        preloadExecutor(new Executor(METADATA_H_THREAD_NUM_PRELOAD,
        METADATA_H_QUEUE_SIZE,
                                     "Preload")),
        batchExecutor(new Executor(METADATA_H_THREAD_NUM_BATCH,
        METADATA_H_QUEUE_SIZE_BATCH,
                                   "Batch")) {

}

void Metadata::resetBatch() {
    for (uint32_t i = 0; i < joinsInBatch; ++i) {
        if (batch[i] != nullptr) {
            delete batch[i];
        }
    }
    joinsInBatch = 0;
    if (activeJoin != nullptr) {
        delete activeJoin;
        activeJoin = nullptr;
    }
}

Metadata::~Metadata() {
    if (batch != nullptr) {
        resetBatch();
        delete[] batch;
    }
    delete hashExecutor;
    delete joinExecutor;
    delete preloadExecutor;
    delete batchExecutor;
}

void Metadata::addTable(uint32_t table) {
    if (activeJoin == nullptr) {
        throw runtime_error("addTable: No active join");
    }
    activeJoin->addTable(table);
}
void Metadata::addTableRelationship(uint32_t tableA,
                                    size_t columnA,
                                    uint32_t tableB,
                                    size_t columnB) {
    if (activeJoin == nullptr) {
        throw runtime_error("addTableRelationship: No active join");
    }
    activeJoin->addTableRelationship(tableA, columnA, tableB, columnB);
}
void Metadata::addTableFilter(uint32_t table,
                              size_t column,
                              uint64_t filterNumber,
                              char type) {
    if (activeJoin == nullptr) {
        throw runtime_error("addTableFilter: No active join");
    }
    activeJoin->addTableFilter(table, column, filterNumber, type);
}
void Metadata::addSumColumn(uint32_t table, size_t column) {
    if (activeJoin == nullptr) {
        throw runtime_error("addSumColumn: No active join");
    }
    activeJoin->addSumColumn(table, column);
}

void Metadata::startJoin() {
    if (activeJoin != nullptr) {
        throw runtime_error("startJoin: Join already active");
    }
    if (joinsInBatch >= arraySize) {
        throw runtime_error("startJoin: limit reached, can't add more joins");
    }
    activeJoin = new Join(tableLoader,
                          *hashExecutor,
                          *joinExecutor,
                          *preloadExecutor,
                          arraySize);
    batch[joinsInBatch++] = new BatchJoinJob(activeJoin);
}
void Metadata::endJoin() {
    if (activeJoin == nullptr) {
        throw runtime_error("endJoin: No join currently active");
    }
    activeJoin = nullptr;
    batchExecutor->addToQueue(batch[joinsInBatch - 1]);
}
void Metadata::endBatch() {
    if (activeJoin != nullptr) {
        endJoin();
    }
    for (uint32_t i = 0; i < joinsInBatch; ++i) {
        JoinSumResult& joinSums = *(batch[i]->waitAndGetResult());
        uint32_t numSums = joinSums.getNumOfSums();
        if (joinSums.getHasResults()) {
            for (uint32_t j = 0; j < numSums; ++j) {
                cout << joinSums.getSum(j) << " ";
            }
            cout << endl;
        }
        else if (joinSums.getNumOfSums() == 0) {
            cout << "NULL" << endl;
        }
        else {
            for (uint32_t j = 0; j < numSums; ++j) {
                cout << "NULL ";
            }
            cout << endl;
        }
        delete batch[i];
        batch[i] = nullptr;
    }
    resetBatch();
}

ostream& operator<<(ostream& os, const Metadata& toPrint) {
    os << "[Metadata tableLoader=" << toPrint.tableLoader << ", batch=";
    if (toPrint.batch == nullptr) {
        os << "null";
    }
    else {
        os << "[";
        for (uint32_t i = 0; i < toPrint.joinsInBatch; ++i) {
            if (i != 0) {
                os << ", ";
            }
            os << toPrint.batch[i];
        }
        os << "]";
    }
    os << ", activeJoin=";
    if (toPrint.activeJoin == nullptr) {
        os << "null";
    }
    else {
        os << *(toPrint.activeJoin);
    }
    os << ", arraySize="
       << toPrint.arraySize
       << ", joinsInBatch="
       << toPrint.joinsInBatch
       << "]";
    return os;
}
