/*
 * ResultContainer.h
 *
 *  Created on: 29 Οκτ 2018
 *      Author: pibre
 */

#ifndef RESULTCONTAINER_H_
#define RESULTCONTAINER_H_

#include <string>

#include <cstdint>

#include "Result.h"

class ResultContainer {
protected:
    Result* start;
    Result* end;
public:
    ResultContainer();
    ResultContainer(const ResultContainer& toCopy);
    ResultContainer(ResultContainer&& toMove);
    ResultContainer& operator=(const ResultContainer& toCopy);
    ResultContainer& operator=(ResultContainer&& toMove);
    virtual ~ResultContainer();

    void addTuple(Tuple& toAdd);

    friend std::ostream& operator<<(std::ostream& os,
                                    const ResultContainer& toPrint);
};

#endif /* RESULTCONTAINER_H_ */
