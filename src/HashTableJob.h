/*
 * HashTableJob.h
 *
 *  Created on: 7 Ιαν 2019
 *      Author: pibre
 */

#ifndef HASHTABLEJOB_H_
#define HASHTABLEJOB_H_
class HashTableJob;

#include <ostream>

#include "Callable.h"
#include "HashTable.h"

class HashTableJob: public Callable<const HashTable> {
protected:
    Executor& executor;
    const Relation& relation;
    const HashFunction& hashFunction;
    HashTable* hashTable;

    void printSelf(std::ostream& os) const;
    const HashTable* getResultInternal();
    void runInternal();
public:
    HashTableJob(Executor& executor,
                 const Relation& relation,
                 const HashFunction& hashFunction);
    HashTableJob(const HashTableJob& toCopy) = delete;
    HashTableJob(HashTableJob&& toMove) = delete;
    HashTableJob& operator=(const HashTableJob& toCopy) = delete;
    HashTableJob& operator=(HashTableJob&& toMove) = delete;
    virtual ~HashTableJob();

    friend std::ostream& operator<<(std::ostream& os,
                                    const HashTableJob& toPrint);
};

#endif /* HASHTABLEJOB_H_ */
