// Name        : part_1.cpp
// Author      : sdi1100070
// Version     :
// Copyright   : No copyright
// Description : Part 1 of Devel project
//============================================================================

#include <iostream>
#include <string>

#include <cstdint>
#include <cmath>
#include <ctime>

#include "ConsoleOutput.h"
#include "BucketAndChain.h"
#include "Tuple.h"
#include "HashTable.h"
#include "Relation.h"
#include "ResultContainer.h"
#include "TableLoader.h"
#include "Metadata.h"

#include <sys/time.h>

using namespace std;

enum ParseState {
    START = 0,
    TABLE = 1,
    TABLE_COL_START = 2,
    TABLE_COL = 3,
    CONDITION = 4,
    TABLE_TWO = 5,
    TABLE_TWO_COL_START = 6,
    TABLE_TWO_COL = 7
};

void removeTrailingNewlines(string& toProcess);
bool processInputFile(string& input, TableLoader& tableLoader);
bool processInputJoin(string& input, Metadata& metadata);
void addTableIfNecessary(ConsoleOutput& consoleOutput,
                         Metadata& metadata,
                         uint32_t& tableNum,
                         ParseState& parseState,
                         char current,
                         string::iterator strIter,
                         string input);
void advanceStateDigit(ParseState& parseState,
                       char current,
                       string::iterator strIter,
                       string input);
void addRelationOrFilter(ConsoleOutput& consoleOutput,
                         ParseState& parseState,
                         uint32_t tableNum,
                         size_t tableCol,
                         char condition,
                         uint32_t tableTwo,
                         Metadata& metadata,
                         uint64_t& currNum,
                         char current,
                         string::iterator strIter,
                         string input);
void handeDotCondition(ParseState& parseState,
                       uint32_t& tableNum,
                       uint64_t& currNum,
                       uint32_t& tableTwo,
                       char current,
                       string::iterator strIter,
                       string input);
void handleCondition(ParseState& parseState,
                     char current,
                     size_t& tableCol,
                     uint64_t& currNum,
                     char& condition,
                     string::iterator strIter,
                     string input);
void addSumColumnIfNecessary(const ConsoleOutput& consoleOutput,
                             ParseState& parseState,
                             uint32_t tableNum,
                             char current,
                             string::iterator strIter,
                             Metadata& metadata,
                             uint32_t& currNum,
                             string& input);
void handleDotSum(ParseState& parseState,
                  char current,
                  string::iterator strIter,
                  uint32_t& tableNum,
                  uint32_t& currNum,
                  string& input);
void parseTables(string::iterator& strIter,
                 string::iterator& iterEnd,
                 ConsoleOutput& consoleOutput,
                 Metadata& metadata,
                 string& input);
void parseRelationshipsAndFilters(string::iterator& strIter,
                                  string::iterator& iterEnd,
                                  const ConsoleOutput& consoleOutput,
                                  Metadata& metadata,
                                  string& input);
void parseSums(string::iterator& strIter,
               string::iterator& iterEnd,
               const ConsoleOutput& consoleOutput,
               Metadata& metadata,
               string& input);

int main(int argc, char* argv[]) {
    ConsoleOutput::debugEnabledDefault = true;
    ConsoleOutput consoleOutput("Main");

    try {
        consoleOutput.errorOutput() << "PART_1 EXECUTION STARTED" << endl;
        struct timespec start;
        clock_gettime(CLOCK_MONOTONIC, &start);

        TableLoader tableLoader(100);

        //Throw exception on failure
        cin.exceptions(iostream::failbit | iostream::badbit);

        //For ease of testing, get input from arguments
        int currArg = 1;
        if (currArg < argc) {
            string input;
            for (; currArg < argc; ++currArg) {
                CO_IFDEBUG(consoleOutput, "Processing arg " << currArg);
                input = argv[currArg];

                if (processInputFile(input, tableLoader))
                    break;
            }

            CO_IFDEBUG(consoleOutput, "Loaded tables from args");
        }
        else {
            string input;
            do {
                cerr << "Give the path to an input file or write 'Done' to stop giving input files: "
                     << endl;
                getline(cin, input);
                if (cin.fail() || cin.eof()) {
                    throw runtime_error("cin terminated unexpectedly");
                }

            } while (!processInputFile(input, tableLoader));

            CO_IFDEBUG(consoleOutput, "Loaded tables from cin");
        }

        //CO_IFDEBUG(consoleOutput, "Loaded tables " << tableLoader);

        struct timespec joinStart;
        clock_gettime(CLOCK_MONOTONIC, &joinStart);

        //For ease of testing, get input from arguments
        Metadata metadata(tableLoader, 100);
        ++currArg;
        if (currArg < argc) {
            string input;
            for (; currArg < argc; ++currArg) {
                CO_IFDEBUG(consoleOutput, "Processing arg " << currArg);
                input = argv[currArg];

                if (processInputJoin(input, metadata))
                    break;
            }
            if (currArg == argc) {
                input = "Done";
                processInputJoin(input, metadata);
            }

            CO_IFDEBUG(consoleOutput, "Finished processing args");
        }
        else {
            string input;
            do {
                cerr << "Give a join expression, F to end the batch or Done to exit: "
                     << endl;
                if (cin.eof()) {
                    input = "Done";
                }
                else {
                    getline(cin, input);
                }
                if (cin.fail()) {
                    throw runtime_error("cin terminated unexpectedly");
                }
            } while (!processInputJoin(input, metadata));

            CO_IFDEBUG(consoleOutput, "Finished processing cin");
        }

        struct timespec end;
        clock_gettime(CLOCK_MONOTONIC, &end);

        consoleOutput.errorOutput() << "PART_1 EXECUTION ENDED" << endl;
        consoleOutput.errorOutput() << "Load Time: "
                                    << ((joinStart.tv_sec - start.tv_sec)
            + ((joinStart.tv_nsec - start.tv_nsec) / 1000000000.0))
                                    << endl;
        consoleOutput.errorOutput() << "Join Time: "
                                    << ((end.tv_sec - joinStart.tv_sec)
                                        + ((end.tv_nsec - joinStart.tv_nsec)
                                           / 1000000000.0))
                                    << endl;
        consoleOutput.errorOutput() << "Total Time: "
                                    << ((end.tv_sec - start.tv_sec)
                                        + ((end.tv_nsec - start.tv_nsec)
                                           / 1000000000.0))
                                    << endl;

        return 0;
    }
    catch (const exception& ex) {
        cerr << "Error occurred: " << ex.what() << endl;
    }
    catch (...) {
        cerr << "Unknown failure occurred" << endl;
    }
}

void removeTrailingNewlines(string& toProcess) {
    while (toProcess.back() == '\n') {
        toProcess.pop_back();
    }
}

/** return true if end was reached **/
bool processInputFile(string& input, TableLoader& tableLoader) {
    ConsoleOutput consoleOutput("processInputFile");
    CO_IFDEBUG(consoleOutput, "Got input: '" << input << "'");
    removeTrailingNewlines(input);
    if (input == "Done") {
        CO_IFDEBUG(consoleOutput, "Got Done. Finished loading files.");
        return true;
    }
    CO_IFDEBUG(consoleOutput, "Loading file '" << input << "'");
    tableLoader.loadTable(input);
    return false;
}

void parseTables(string::iterator& strIter,
                 string::iterator& iterEnd,
                 ConsoleOutput& consoleOutput,
                 Metadata& metadata,
                 string& input) {
    uint32_t tableNum = 0;
    ParseState parseState = ParseState::START;
    while (true) {
        if (strIter == iterEnd) {
            CO_IFDEBUG(consoleOutput, "Reached end of string");
            addTableIfNecessary(consoleOutput,
                                metadata,
                                tableNum,
                                parseState,
                                '\0',
                                strIter,
                                input);
            break;
        }
        char current = *strIter;
        CO_IFDEBUG(consoleOutput, "Processing character '" << current << "'");
        if (current >= '0' && current <= '9') {
            CO_IFDEBUG(consoleOutput, "Found number");
            tableNum = tableNum * 10 + (current - '0');
            advanceStateDigit(parseState, current, strIter, input);
        }
        else if (current == ' ') {
            CO_IFDEBUG(consoleOutput, "Found space");
            addTableIfNecessary(consoleOutput,
                                metadata,
                                tableNum,
                                parseState,
                                current,
                                strIter,
                                input);
        }
        else if (current == '|') {
            CO_IFDEBUG(consoleOutput, "Found pipe, advancing one");
            addTableIfNecessary(consoleOutput,
                                metadata,
                                tableNum,
                                parseState,
                                current,
                                strIter,
                                input);
            ++strIter;
            break;
        }
        else {
            throw runtime_error("parseTables: encountered unexpected character [state="
                                + to_string(parseState)
                                + ", current="
                                + to_string(current)
                                + ", position="
                                + to_string(strIter - input.begin())
                                + ", input="
                                + input
                                + "]");
        }

        ++strIter;
    }
}

void parseRelationshipsAndFilters(string::iterator& strIter,
                                  string::iterator& iterEnd,
                                  ConsoleOutput& consoleOutput,
                                  Metadata& metadata,
                                  string& input) {
    uint32_t tableNum = 0;
    size_t tableCol = 0;
    uint32_t tableTwo = 0;
    ParseState parseState = ParseState::START;
    uint64_t currNum = 0;
    char condition = '\0';
    while (true) {
        if (strIter == iterEnd) {
            CO_IFDEBUG(consoleOutput, "Reached end of string");
            addRelationOrFilter(consoleOutput,
                                parseState,
                                tableNum,
                                tableCol,
                                condition,
                                tableTwo,
                                metadata,
                                currNum,
                                '\0',
                                strIter,
                                input);
            break;
        }
        char current = *strIter;
        CO_IFDEBUG(consoleOutput, "Processing character '" << current << "'");
        if (current >= '0' && current <= '9') {
            CO_IFDEBUG(consoleOutput, "Found number");
            currNum = currNum * 10 + (current - '0');
            advanceStateDigit(parseState, current, strIter, input);
        }
        else if (current == '.') {
            CO_IFDEBUG(consoleOutput, "Found dot");
            handeDotCondition(parseState,
                              tableNum,
                              currNum,
                              tableTwo,
                              current,
                              strIter,
                              input);
        }
        else if (current == '=' || current == '<' || current == '>') {
            CO_IFDEBUG(consoleOutput, "Found condition");
            handleCondition(parseState,
                            current,
                            tableCol,
                            currNum,
                            condition,
                            strIter,
                            input);
        }
        else if (current == '&') {
            CO_IFDEBUG(consoleOutput, "Found ampersand");
            if (parseState == ParseState::START) {
                throw runtime_error("parseRelationshipsAndFilters: no number before ampersand [state="
                                    + to_string(parseState)
                                    + ", current="
                                    + to_string(current)
                                    + ", position="
                                    + to_string(strIter - input.begin())
                                    + ", input="
                                    + input
                                    + "]");
            }
            addRelationOrFilter(consoleOutput,
                                parseState,
                                tableNum,
                                tableCol,
                                condition,
                                tableTwo,
                                metadata,
                                currNum,
                                current,
                                strIter,
                                input);
        }
        else if (current == '|') {
            CO_IFDEBUG(consoleOutput, "Found pipe, advancing one");
            addRelationOrFilter(consoleOutput,
                                parseState,
                                tableNum,
                                tableCol,
                                condition,
                                tableTwo,
                                metadata,
                                currNum,
                                current,
                                strIter,
                                input);
            ++strIter;
            break;
        }
        else {
            throw runtime_error("parseRelationshipsAndFilters: encountered unexpected character [state="
                                + to_string(parseState)
                                + ", current="
                                + to_string(current)
                                + ", position="
                                + to_string(strIter - input.begin())
                                + ", input="
                                + input
                                + "]");
        }

        ++strIter;
    }
}

void parseSums(string::iterator& strIter,
               string::iterator& iterEnd,
               const ConsoleOutput& consoleOutput,
               Metadata& metadata,
               string& input) {
    ParseState parseState = ParseState::START;
    uint32_t currNum = 0;
    uint32_t tableNum = 0;
    while (true) {
        if (strIter == iterEnd) {
            CO_IFDEBUG(consoleOutput, "Reached end of string");
            addSumColumnIfNecessary(consoleOutput,
                                    parseState,
                                    tableNum,
                                    '\0',
                                    strIter,
                                    metadata,
                                    currNum,
                                    input);
            break;
        }
        char current = *strIter;
        CO_IFDEBUG(consoleOutput, "Processing character '" << current << "'");
        if (current >= '0' && current <= '9') {
            CO_IFDEBUG(consoleOutput, "Found number");
            currNum = currNum * 10 + (current - '0');
            advanceStateDigit(parseState, current, strIter, input);
        }
        else if (current == '.') {
            CO_IFDEBUG(consoleOutput, "Found dot");
            handleDotSum(parseState,
                         current,
                         strIter,
                         tableNum,
                         currNum,
                         input);
        }
        else if (current == ' ') {
            CO_IFDEBUG(consoleOutput, "Found space");
            addSumColumnIfNecessary(consoleOutput,
                                    parseState,
                                    tableNum,
                                    current,
                                    strIter,
                                    metadata,
                                    currNum,
                                    input);
        }
        else {
            throw runtime_error("parseSum: encountered unexpected character [state="
                                + to_string(parseState)
                                + ", current="
                                + to_string(current)
                                + ", position="
                                + to_string(strIter - input.begin())
                                + ", input="
                                + input
                                + "]");
        }

        ++strIter;
    }
}

/** return true if end was reached **/
bool processInputJoin(string& input, Metadata& metadata) {
    ConsoleOutput consoleOutput("processInputJoin");
    CO_IFDEBUG(consoleOutput, "Got input: '" << input << "'");
    removeTrailingNewlines(input);
    if (input == "Done") {
        CO_IFDEBUG(consoleOutput, "Got Done, ending batch and ending execution");
        metadata.endBatch();
        return true;
    }
    if (input == "F") {
        CO_IFDEBUG(consoleOutput, "Got F, ending batch");
        metadata.endBatch();
        return false;
    }

    metadata.startJoin();

    string::iterator strIter = input.begin();
    string::iterator iterEnd = input.end();

    CO_IFDEBUG(consoleOutput, "Parsing tables");
    parseTables(strIter, iterEnd, consoleOutput, metadata, input);

    CO_IFDEBUG(consoleOutput, "Parsing relationships and filters");
    parseRelationshipsAndFilters(strIter,
                                 iterEnd,
                                 consoleOutput,
                                 metadata,
                                 input);

    CO_IFDEBUG(consoleOutput, "Parsing sums");
    parseSums(strIter, iterEnd, consoleOutput, metadata, input);

    metadata.endJoin();
    return false;
}

void addTableIfNecessary(ConsoleOutput& consoleOutput,
                         Metadata& metadata,
                         uint32_t& tableNum,
                         ParseState& parseState,
                         char current,
                         string::iterator strIter,
                         string input) {
    CO_IFDEBUG(consoleOutput,
               "addTableIfNecessary [tableNum=" << tableNum << ", parseState=" << parseState << "]");
    switch (parseState) {
    case START:
        //Do nothing
        break;
    case TABLE:
        metadata.addTable(tableNum);
        tableNum = 0;
        parseState = ParseState::START;
        break;
    default:
        throw runtime_error("addTableIfNecessary: unknown state [state="
                            + to_string(parseState)
                            + ", current="
                            + to_string(current)
                            + ", position="
                            + to_string(strIter - input.begin())
                            + ", input="
                            + input
                            + "]");
        break;
    }
}

void advanceStateDigit(ParseState& parseState,
                       char current,
                       string::iterator strIter,
                       string input) {
    switch (parseState) {
    case START:
        parseState = ParseState::TABLE;
        break;
    case TABLE:
        break;
    case TABLE_COL_START:
        parseState = ParseState::TABLE_COL;
        break;
    case TABLE_COL:
        break;
    case CONDITION:
        parseState = ParseState::TABLE_TWO;
        break;
    case TABLE_TWO:
        break;
    case TABLE_TWO_COL_START:
        parseState = ParseState::TABLE_TWO_COL;
        break;
    case TABLE_TWO_COL:
        break;
    default:
        throw runtime_error("advanceStateDigit: unknown state [state="
                            + to_string(parseState)
                            + ", current="
                            + to_string(current)
                            + ", position="
                            + to_string(strIter - input.begin())
                            + ", input="
                            + input
                            + "]");
        break;
    }
}

void addRelationOrFilter(ConsoleOutput& consoleOutput,
                         ParseState& parseState,
                         uint32_t tableNum,
                         size_t tableCol,
                         char condition,
                         uint32_t tableTwo,
                         Metadata& metadata,
                         uint64_t& currNum,
                         char current,
                         string::iterator strIter,
                         string input) {
    CO_IFDEBUG(consoleOutput,
               "addRelationOrFilter [tableNum="<<tableNum<<", tableCol="<<tableCol<<", condition="<<condition<<", tableTwo="<<tableTwo<<", currNum="<<currNum<<", parseState="<<parseState<<"]");
    switch (parseState) {
    case START:
        //Do nothing
        break;
    case TABLE_TWO:
        metadata.addTableFilter(tableNum, tableCol, currNum, condition);
        break;
    case TABLE_TWO_COL:
        if (condition != '=') {
            throw runtime_error("Only equality (=) can be used to link two tables");
        }
        metadata.addTableRelationship(tableNum, tableCol, tableTwo, currNum);
        break;
    default:
        throw runtime_error("addRelationOrFilter: unknown state [state="
                            + to_string(parseState)
                            + ", current="
                            + to_string(current)
                            + ", position="
                            + to_string(strIter - input.begin())
                            + ", input="
                            + input
                            + "]");
        break;
    }
    currNum = 0;
    parseState = ParseState::START;
}

void handeDotCondition(ParseState& parseState,
                       uint32_t& tableNum,
                       uint64_t& currNum,
                       uint32_t& tableTwo,
                       char current,
                       string::iterator strIter,
                       string input) {
    switch (parseState) {
    case TABLE:
        tableNum = currNum;
        parseState = ParseState::TABLE_COL_START;
        break;
    case TABLE_TWO:
        tableTwo = currNum;
        parseState = ParseState::TABLE_TWO_COL_START;
        break;
    default:
        throw runtime_error("handleDotCondition: unknown state [state="
                            + to_string(parseState)
                            + ", current="
                            + to_string(current)
                            + ", position="
                            + to_string(strIter - input.begin())
                            + ", input="
                            + input
                            + "]");
        break;
    }
    currNum = 0;
}

void handleCondition(ParseState& parseState,
                     char current,
                     size_t& tableCol,
                     uint64_t& currNum,
                     char& condition,
                     string::iterator strIter,
                     string input) {
    switch (parseState) {
    case TABLE_COL:
        tableCol = currNum;
        condition = current;
        parseState = ParseState::CONDITION;
        break;
    default:
        throw runtime_error("handleCondition: unknown state [state="
                            + to_string(parseState)
                            + ", current="
                            + to_string(current)
                            + ", position="
                            + to_string(strIter - input.begin())
                            + ", input="
                            + input
                            + "]");
        break;
    }
    currNum = 0;
}

void addSumColumnIfNecessary(const ConsoleOutput& consoleOutput,
                             ParseState& parseState,
                             uint32_t tableNum,
                             char current,
                             string::iterator strIter,
                             Metadata& metadata,
                             uint32_t& currNum,
                             string& input) {
    CO_IFDEBUG(consoleOutput,
               "addSumColumnIfNecessary [tableNum="<<tableNum<<", currNum="<<currNum<<", parseState="<<parseState<<"]");
    switch (parseState) {
    case START:
        //Do nothing
        break;
    case TABLE_COL:
        metadata.addSumColumn(tableNum, currNum);
        break;
    default:
        throw runtime_error("addSumColumnIfNecessary: unknown state [state="
                            + to_string(parseState)
                            + ", current="
                            + to_string(current)
                            + ", position="
                            + to_string(strIter - input.begin())
                            + ", input="
                            + input
                            + "]");
        break;
    }
    currNum = 0;
    parseState = ParseState::START;
}

void handleDotSum(ParseState& parseState,
                  char current,
                  string::iterator strIter,
                  uint32_t& tableNum,
                  uint32_t& currNum,
                  string& input) {
    switch (parseState) {
    case TABLE:
        tableNum = currNum;
        break;
    default:
        throw runtime_error("handeDotSum: unknown state [state="
                            + to_string(parseState)
                            + ", current="
                            + to_string(current)
                            + ", position="
                            + to_string(strIter - input.begin())
                            + ", input="
                            + input
                            + "]");
        break;
    }
    currNum = 0;
    parseState = ParseState::TABLE_COL_START;
}
