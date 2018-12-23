/*
 * MultipleColumnStatsPair.h
 *
 *  Created on: 23 Δεκ 2018
 *      Author: parisbre56
 */

#ifndef MULTIPLECOLUMNSTATSPAIR_H_
#define MULTIPLECOLUMNSTATSPAIR_H_
class MultipleColumnStatsPair;

#include "MultipleColumnStats.h"

class MultipleColumnStatsPair {
protected:
    MultipleColumnStats* left;
    MultipleColumnStats* right;
public:
    /** Creates a pair containing copies of the two given stats **/
    MultipleColumnStatsPair(const MultipleColumnStats& left,
                            const MultipleColumnStats& right);
    MultipleColumnStatsPair(const MultipleColumnStatsPair& toCopy) = delete;
    MultipleColumnStatsPair(MultipleColumnStatsPair&& toMove);
    MultipleColumnStatsPair& operator=(const MultipleColumnStatsPair& toCopy) = delete;
    MultipleColumnStatsPair& operator=(MultipleColumnStatsPair&& toMove) = delete;
    virtual ~MultipleColumnStatsPair();

    MultipleColumnStats& getLeft();
    MultipleColumnStats& getRight();
    const MultipleColumnStats& getLeftConst() const;
    const MultipleColumnStats& getRightConst() const;

    friend std::ostream& operator<<(std::ostream& os,
                                    const MultipleColumnStatsPair& toPrint);
};

#endif /* MULTIPLECOLUMNSTATSPAIR_H_ */
