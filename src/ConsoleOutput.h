/*
 * ConsoleOutput.h
 *
 *  Created on: 21 Ïêô 2018
 *      Author: parisbre56
 */

#ifndef CONSOLEOUTPUT_H_
#define CONSOLEOUTPUT_H_

#include <string>

class ConsoleOutput {
private:
    bool debugEnabled;
public:
    ConsoleOutput() = delete;
    ConsoleOutput(bool debugEnabled);
    virtual ~ConsoleOutput();

    bool getDebugEnabled();
    void setDebugEnabled(bool debugEnabled);

    void debugOutput(std::string outString);
    void errorOutput(std::string outString);
};

#endif /* CONSOLEOUTPUT_H_ */
