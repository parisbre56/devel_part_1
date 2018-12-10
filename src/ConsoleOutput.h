/*
 * ConsoleOutput.h
 *
 *  Created on: 21 ��� 2018
 *      Author: parisbre56
 */

#ifndef CONSOLEOUTPUT_H_
#define CONSOLEOUTPUT_H_
class ConsoleOutput;

#include <string>
#include <ostream>

//Only executes debug code if debug is enabled
#ifdef NDEBUG
#define CO_IFDEBUG(consoleOutput, toPrint) ((void)0); //NOOP
#else
#define CO_IFDEBUG(consoleOutput, toPrint) if(consoleOutput.getDebugEnabled()) {consoleOutput.debugOutput() << toPrint << std::endl;}
#endif //NDEBUG

class ConsoleOutput {
protected:
    bool debugEnabled;
    std::string label;
public:
    static bool debugEnabledDefault;

    explicit ConsoleOutput(std::string label);
    ConsoleOutput(std::string label, bool debugEnabled);
    virtual ~ConsoleOutput();

    bool const getDebugEnabled() const;
    void setDebugEnabled(bool debugEnabled);

    std::ostream& debugOutput() const;
    std::ostream& errorOutput() const;
};

#endif /* CONSOLEOUTPUT_H_ */
