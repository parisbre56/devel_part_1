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
#include <sstream>

//Only executes debug code if debug is enabled
#ifdef NDEBUG
#define CO_IFDEBUG(consoleOutput, toPrint) ((void)0); //NOOP
#else
#define CO_IFDEBUG(consoleOutput, toPrint) if(consoleOutput.getDebugEnabled()) {consoleOutput.debugOutput() << toPrint << std::endl;}
#endif //NDEBUG

#define CO_ERROR(consoleOutput, toPrint) {std::stringstream errString; errString << toPrint; consoleOutput.errorOutput(errString.str());}

class ConsoleOutput {
protected:
    bool debugEnabled;
    const std::string label;
public:
    static bool debugEnabledDefault;

    explicit ConsoleOutput(std::string label);
    ConsoleOutput(std::string label, bool debugEnabled);
    virtual ~ConsoleOutput();

    bool const getDebugEnabled() const;
    void setDebugEnabled(bool debugEnabled);

    std::ostream& debugOutput() const;
    std::ostream& errorOutput() const;
    void debugOutput(std::string toPrint) const;
    void errorOutput(std::string toPrint) const;
};

#endif /* CONSOLEOUTPUT_H_ */
