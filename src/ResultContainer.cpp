/*
 * ResultContainer.cpp
 *
 *  Created on: 29 Οκτ 2018
 *      Author: pibre
 */

#include "ResultContainer.h"

#include <sstream>

#include "ConsoleOutput.h"

using namespace std;

ResultContainer::ResultContainer() {
    start = new Result();
    end = start;
}

ResultContainer::ResultContainer(const ResultContainer& toCopy) {
    start = new Result(*(toCopy.start));
    end = start->getFirstNonFullSegment();
}

ResultContainer::ResultContainer(ResultContainer&& toMove) {
    start = toMove.start;
    end = toMove.end;
    toMove.start = nullptr;
    toMove.end = nullptr;
}

ResultContainer& ResultContainer::operator=(const ResultContainer& toCopy) {
    if (start != nullptr) {
        delete start;
    }
    start = new Result(*(toCopy.start));
    end = start->getFirstNonFullSegment();
    return *this;
}

ResultContainer& ResultContainer::operator=(ResultContainer&& toMove) {
    if (start != nullptr) {
        delete start;
    }
    start = toMove.start;
    end = toMove.end;
    toMove.start = nullptr;
    toMove.end = nullptr;
    return *this;
}
ResultContainer::~ResultContainer() {
    if (start != nullptr) {
        delete start;
    }
}

void ResultContainer::addTuple(Tuple& toAdd) {
    Result* nextEnd = end->addTuple(toAdd);
    if (nextEnd != nullptr) {
        end = nextEnd;
    }
}

std::ostream& operator<<(std::ostream& os,
                                const ResultContainer& toPrint) {
    os << "[ResultContainer start=";
    if (toPrint.start == nullptr) {
        os << "null";
    }
    else {
        os << toPrint.start;
    }
    os << ", end=";
    if (toPrint.end == nullptr) {
        os << "null";
    }
    else {
        os << toPrint.end;
    }
    os << "]";
    return os;
}

