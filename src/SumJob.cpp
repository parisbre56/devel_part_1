/*
 * SumJob.cpp
 *
 *  Created on: 8 Ιαν 2019
 *      Author: pibre
 */

#include "SumJob.h"

using namespace std;

SumJob::SumJob(Executor& executor,
               const Result& startResult,
               const uint32_t tableNum,
               const uint64_t* const tableColumn) :
        executor(executor),
        startResult(startResult),
        tableNum(tableNum),
        tableColumn(tableColumn),
        child(generateChild(executor, startResult, tableNum, tableColumn)),
        result(0) {

}

SumJob::~SumJob() {
    if (child != nullptr) {
        delete child;
    }
}

SumJob* SumJob::generateChild(Executor& executor,
                      const Result& startResult,
                      const uint32_t tableNum,
                      const uint64_t* const tableColumn) {
    Result* nextResult = startResult.getNext();
    while (nextResult != nullptr
           && nextResult->getRelation().getNumTuples() == 0) {
        nextResult = nextResult->getNext();
    }
    if (nextResult == nullptr) {
        return nullptr;
    }
    return new SumJob(executor, *nextResult, tableNum, tableColumn);
}
void SumJob::printSelf(ostream& os) const {
    os << "[SumJob finished="
       << finished
       << ", startResult.relation="
       << startResult.getRelation()
       << ", tableNum="
       << tableNum
       << ", tableColumn="
       << (void*) tableColumn
       << ", child="
       << (void*) child
       << ", result="
       << result
       << "]";
}
const uint64_t* SumJob::getResultInternal() {
    //If we haven't waited for the child, do so
    if (child != nullptr) {
        result += *(child->waitAndGetResult());
        delete child;
        child = nullptr;
    }
    return &result;
}
void SumJob::runInternal() {
    if (child != nullptr) {
        executor.addToQueue(child);
    }
    const Relation& startRelation = startResult.getRelation();
    const uint64_t numTuples = startRelation.getNumTuples();
    const Tuple* const * const tuples = startRelation.getTuples();
    for (uint64_t i = 0; i < numTuples; ++i) {
        result += tableColumn[tuples[i]->getTableRow(tableNum)];
    }
}

ostream& operator<<(ostream& os, const SumJob& toPrint) {
    toPrint.printSelf(os);
    return os;
}
