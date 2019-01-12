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

#include <unistd.h>
#include <sys/types.h>
#ifdef __linux__
#include <sys/syscall.h>
#endif

using namespace std;
using namespace std::chrono;

ostream nullout(nullptr);

bool ConsoleOutput::debugEnabledDefault = true;

//Constructors
ConsoleOutput::ConsoleOutput(string label) :
        ConsoleOutput(label, debugEnabledDefault) {

}

ConsoleOutput::ConsoleOutput(string label, bool debugEnabled) :
        debugEnabled(debugEnabled), label(label) {
}

ConsoleOutput::~ConsoleOutput() {
    //Nothing to clean
}

//Getters/Setters
bool const ConsoleOutput::getDebugEnabled() const {
    return debugEnabled;
}

void ConsoleOutput::setDebugEnabled(bool debugEnabled) {
    this->debugEnabled = debugEnabled;
}

//Methods
ostream& output(ostream& stream, string level, string label) {
    //Get now
    time_point<system_clock> now = system_clock::now();
    //Get current millis
    duration<long int, ratio<1, 1000>> ms = duration_cast<milliseconds>(now.time_since_epoch())
                                            % 1000;
    //Get c time
    time_t cnow = system_clock::to_time_t(now);
    tm localt = *localtime(&cnow);

    //Get thread id
#ifdef __linux__
    pid_t tid = syscall(SYS_gettid);
#endif
    stream << put_time(&localt, "%H:%M:%S")
           << '.'
           << setfill('0')
           << setw(3)
           << ms.count()
           << " "
           << level
#ifdef __linux__
           << " ("
           << tid
           << ")"
#endif
           << " ["
           << label
           << "] ";
    return stream;
}

ostream& ConsoleOutput::debugOutput() const {
    if (!debugEnabled) {
        return nullout;
    }

    return output(cerr, "DEBUG", label);
}
ostream& ConsoleOutput::errorOutput() const {
    return output(cerr, "ERROR", label);
}

void ConsoleOutput::debugOutput(string toPrint) const {
    stringstream out;
    output(out, "DEBUG", label);
    out << toPrint;
    cerr << out.str() << endl;
}
void ConsoleOutput::errorOutput(string toPrint) const {
    stringstream out;
    output(out, "ERROR", label);
    out << toPrint;
    cerr << out.str() << endl;
}
