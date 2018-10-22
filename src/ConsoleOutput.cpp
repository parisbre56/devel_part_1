/*
 * ConsoleOutput.cpp
 *
 *  Created on: 21 ��� 2018
 *      Author: parisbre56
 */

#include "ConsoleOutput.h"

#include <iostream>
#include <iomanip>
#include <chrono>

using namespace std;
using namespace std::chrono;

//Constructors
ConsoleOutput::ConsoleOutput(bool debugEnabled) {
    this->debugEnabled = debugEnabled;
}

ConsoleOutput::~ConsoleOutput() {
    // TODO Auto-generated destructor stub
}

//Getters/Setters
bool const ConsoleOutput::getDebugEnabled() const {
    return debugEnabled;
}

void ConsoleOutput::setDebugEnabled(bool debugEnabled) {
    this->debugEnabled = debugEnabled;
}

//Methods
void output(ostream& stream, string label, string outString) {
    //Get now
    time_point<system_clock> now = system_clock::now();
    //Get current millis
    duration<long int, ratio<1, 1000>> ms = duration_cast<milliseconds>(now.time_since_epoch())
                                            % 1000;
    //Get c time
    time_t cnow = system_clock::to_time_t(now);
    tm localt = *localtime(&cnow);

    stream << put_time(&localt, "%H:%M:%S")
         << '.'
         << setfill('0')
         << setw(3)
         << ms.count()
         << " "
         << label
         << ": "
         << outString
         << endl;
}

void ConsoleOutput::debugOutput(string outString) const {
    if (!debugEnabled) {
        return;
    }

    output(cerr, "DEBUG", outString);
}
void ConsoleOutput::errorOutput(string outString) const {
    output(cerr, "ERROR", outString);
}

