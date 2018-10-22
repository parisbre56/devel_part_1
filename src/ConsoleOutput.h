/*
 * ConsoleOutput.h
 *
 *  Created on: 21 ��� 2018
 *      Author: parisbre56
 */

#ifndef CONSOLEOUTPUT_H_
#define CONSOLEOUTPUT_H_

#include <string>

class ConsoleOutput {
protected:
    bool debugEnabled;
public:
    ConsoleOutput() = delete;
    ConsoleOutput(bool debugEnabled);
    virtual ~ConsoleOutput();

    bool const getDebugEnabled() const;
    void setDebugEnabled(bool debugEnabled);

    void debugOutput(std::string outString) const;
    void errorOutput(std::string outString) const;
};

#endif /* CONSOLEOUTPUT_H_ */
