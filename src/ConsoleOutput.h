/*
 * ConsoleOutput.h
 *
 *  Created on: 21 ��� 2018
 *      Author: parisbre56
 */

#ifndef CONSOLEOUTPUT_H_
#define CONSOLEOUTPUT_H_

//Only executes debug code if debug is enabled
#ifdef NDEBUG
#define CO_IFDEBUG(consoleOutput, toPrint) ((void)0); //NOOP
#else
#define CO_IFDEBUG(consoleOutput, toPrint) if(consoleOutput.getDebugEnabled()) {consoleOutput.debugOutput(toPrint);}
#endif //NDEBUG

#include <string>

class ConsoleOutput {
protected:
    bool debugEnabled;
    std::string label;
public:
    static bool debugEnabledDefault;

    ConsoleOutput(std::string label);
    ConsoleOutput(bool debugEnabled, std::string label);
    virtual ~ConsoleOutput();

    bool const getDebugEnabled() const;
    void setDebugEnabled(bool debugEnabled);

    void debugOutput(std::string outString) const;
    void errorOutput(std::string outString) const;
};

#endif /* CONSOLEOUTPUT_H_ */
