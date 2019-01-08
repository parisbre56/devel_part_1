/*
 * BatchJoinJob.cpp
 *
 *  Created on: 8 Ιαν 2019
 *      Author: pibre
 */

#include "BatchJoinJob.h"

using namespace std;

BatchJoinJob::BatchJoinJob(Join* join) :
        join(join), result(nullptr) {
    // TODO Auto-generated constructor stub

}

BatchJoinJob::~BatchJoinJob() {
    if (result != nullptr) {
        delete result;
    }
    if (join != nullptr) {
        delete join;
    }
}

void BatchJoinJob::printSelf(std::ostream& os) const {
    os << "[BatchJoinJob finished="
       << finished
       << ", join="
       << join
       << ", result=";
    if (result == nullptr) {
        os << "null";
    }
    else {
        os << *result;
    }
    os << "]";
}
JoinSumResult* BatchJoinJob::getResultInternal() {
    return result;
}
void BatchJoinJob::runInternal() {
    result = new JoinSumResult(join->performJoin());
}

const Join* BatchJoinJob::getJoin() const {
    return join;
}

std::ostream& operator<<(std::ostream& os, const BatchJoinJob& toPrint) {
    toPrint.printSelf(os);
    return os;
}
