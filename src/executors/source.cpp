#include "global.h"
#include <sstream> // Include for stringstream

/**
 * @brief 
 * SYNTAX: SOURCE filename
 */
bool syntacticParseSOURCE()
{
    logger.log("syntacticParseSOURCE");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = SOURCE;
    parsedQuery.sourceFileName = tokenizedQuery[1];
    return true;
}

bool semanticParseSOURCE()
{
    logger.log("semanticParseSOURCE");
    if (!isQueryFile(parsedQuery.sourceFileName))
    {
        cout << "SEMANTIC ERROR: File doesn't exist" << endl;
        return false;
    }
    return true;
}

void executeSOURCE()
{
    logger.log("executeSOURCE");
    ifstream file("../data/" + parsedQuery.sourceFileName + ".ra");
    string line;
    while (getline(file, line))
    {
        tokenizedQuery.clear();
        parsedQuery.clear();
        // Remove commas and tokenize the line
        line.erase(remove(line.begin(), line.end(), ','), line.end());
        stringstream ss(line);
        string token;
        while (ss >> token) {
            tokenizedQuery.push_back(token);
        }

        if (!tokenizedQuery.empty())
        {
            doCommand(); // Execute the command read from the file
        }
    }
    file.close();
    return;
}
