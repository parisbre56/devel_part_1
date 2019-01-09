/*
 * LoadRelationTableSegmentJob.cpp
 *
 *  Created on: 9 Ιαν 2019
 *      Author: pibre
 */

#include "LoadRelationResultJob.h"

using namespace std;

LoadRelationResultJob::LoadRelationResultJob(Executor& executor,
                                             const Result& resultToLoad,
                                             Relation& relationToFill,
                                             uint64_t segmentStart,
                                             const size_t sizePayloads,
                                             const uint64_t * const * const payloadCols,
                                             const uint32_t * const payloadTables) :
        executor(executor),
        resultToLoad(resultToLoad),
        relationToFill(relationToFill),
        segmentStart(segmentStart),
        sizePayloads(sizePayloads),
        payloadCols(payloadCols),
        payloadTables(payloadTables),
        child(createChild(executor,
                          resultToLoad,
                          relationToFill,
                          segmentStart,
                          sizePayloads,
                          payloadCols,
                          payloadTables)) {

}

LoadRelationResultJob::~LoadRelationResultJob() {
    if (child != nullptr) {
        delete child;
    }
}

LoadRelationResultJob* const LoadRelationResultJob::createChild(Executor& executor,
                                                                const Result& resultToLoad,
                                                                Relation& relationToFill,
                                                                uint64_t segmentStart,
                                                                const size_t sizePayloads,
                                                                const uint64_t * const * const payloadCols,
                                                                const uint32_t * const payloadTables) {
    Result* nextResult = resultToLoad.getNext();
    while (nextResult != nullptr
           && nextResult->getRelation().getNumTuples() == 0) {
        nextResult = nextResult->getNext();
    }
    if (nextResult == nullptr) {
        return nullptr;
    }
    return new LoadRelationResultJob(executor,
                                     *nextResult,
                                     relationToFill,
                                     segmentStart
                                     + resultToLoad.getRelation().getNumTuples(),
                                     sizePayloads,
                                     payloadCols,
                                     payloadTables);
}
void LoadRelationResultJob::printSelf(std::ostream& os) const {
    os << "[LoadRelationResultJob finished="
       << finished
       << ", segmentStart="
       << segmentStart
       << ", sizePayloads="
       << sizePayloads
       << ", child="
       << (void*) child
       << "]";
}
void * LoadRelationResultJob::getResultInternal() {
    if (child != nullptr) {
        child->waitAndGetResult();
    }
    return nullptr;
}
void LoadRelationResultJob::runInternal() {
    if (child != nullptr) {
        executor.addToQueue(child);
    }
    const Relation& relationToLoad = resultToLoad.getRelation();
    const uint64_t numTuples = relationToLoad.getNumTuples();
    const Tuple * const * const tuples = relationToLoad.getTuples();
    for (uint64_t i = 0; i < numTuples; ++i) {
        Tuple toAdd(*(tuples[i]), sizePayloads);
        for (size_t j = 0; j < sizePayloads; ++j) {
            toAdd.setPayload(j,
                             payloadCols[j][toAdd.getTableRow(payloadTables[j])]);
        }
        relationToFill.setTuple(segmentStart + i, move(toAdd));
    }
}

bool LoadRelationResultJob::hasChild() const {
    return child != nullptr;
}

ostream& operator<<(ostream& os, const LoadRelationResultJob& toPrint) {
    toPrint.printSelf(os);
    return os;
}
