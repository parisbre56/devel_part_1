/*
 * HashTableJob.cpp
 *
 *  Created on: 7 Ιαν 2019
 *      Author: pibre
 */

#include "HashTableJob.h"

using namespace std;

HashTableJob::HashTableJob(Executor& executor,
                           const Relation& relation,
                           const HashFunction& hashFunction) :
        Callable(),
        executor(executor),
        relation(relation),
        hashFunction(hashFunction),
        hashTable(nullptr) {

}

HashTableJob::~HashTableJob() {
    if (hashTable != nullptr) {
        delete hashTable;
    }
}

void HashTableJob::printSelf(ostream& os) const {
    os << "[HashTableJob finished="
       << finished
       << ", executor="
       << executor
       << ", relation="
       << relation
       << ", hashFunction="
       << hashFunction
       << ", hashTable=";
    if (hashTable == nullptr) {
        os << "null";
    }
    else {
        os << *hashTable;
    }
    os << "]";
}

const HashTable* HashTableJob::getResultInternal() {
    return hashTable;
}

void HashTableJob::runInternal() {
    hashTable = new HashTable(executor, relation, hashFunction);
}

ostream& operator<<(ostream& os, const HashTableJob& toPrint) {
    toPrint.printSelf(os);
    return os;
}
