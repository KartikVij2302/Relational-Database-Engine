#include "global.h"
#include "secondary_index.h"

//Syntax :  INSERT INTO table_name ( col1 = val1, col2 = val2, col3 = val3 … ) 


bool syntacticParseINSERT()
{
    logger.log("syntacticParseINSERT");
    if (tokenizedQuery.size() < 8 || tokenizedQuery[1] != "INTO" || tokenizedQuery[3] != "(" || tokenizedQuery[tokenizedQuery.size() - 1] != ")")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = INSERT;
    parsedQuery.insertRelationName = tokenizedQuery[2];
    parsedQuery.insertColumnNames.clear();
    parsedQuery.insertValues.clear();  
    for (int i = 5; i < tokenizedQuery.size() - 1; i++)
    {
        if (tokenizedQuery[i] == "=")
        {
            if (i + 1 >= tokenizedQuery.size() - 1)
            {
                cout << i<<" SYNTAX ERROR" << endl;
                return false;
            }
            parsedQuery.insertColumnNames.push_back(tokenizedQuery[i - 1]);
            parsedQuery.insertValues.push_back(stoi(tokenizedQuery[i + 1]));
            i+=2;
        }
        else
        {
            cout << i<< " SYNTAX ERROR" << endl;
            return false;
        }
    }
    if (parsedQuery.insertColumnNames.size() != parsedQuery.insertValues.size())
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    return true;
}




bool semanticParseINSERT()
{
    logger.log("semanticParseINSERT");
    if(!tableCatalogue.isTable(parsedQuery.insertRelationName))
    {
        cout << "SEMANTIC ERROR: Table does not exist" << endl;
        return false;
    }
    for(int i=0;parsedQuery.insertColumnNames.size() - 1; i++)
    {
        if(!tableCatalogue.isColumnFromTable(parsedQuery.insertColumnNames[i],parsedQuery.insertRelationName))
        {
            cout << "SEMANTIC ERROR: Column does not exist" << endl;
            return false;
        }
    }
    return true;
}

void executeINSERT()
{
    logger.log("executeINSERT");
    // Get the source table
    Table *sourceTable = tableCatalogue.getTable(parsedQuery.insertRelationName);
    vector<string> columns = sourceTable->getColumnNames();
    vector<int> values;
    // For columns not in the insert query, set the value to 0
    for (int i = 0; i < columns.size(); i++)
    {
        bool found = false;
        for (int j = 0; j < parsedQuery.insertColumnNames.size(); j++)
        {
            if (columns[i] == parsedQuery.insertColumnNames[j])
            {
                values.push_back(parsedQuery.insertValues[j]);
                found = true;
                break;
            }
        }
        if (!found)
        {
            values.push_back(0);
        }
    }
    // Insert the row into the table
    sourceTable->insertRow(values);
    // Check and update index for the indexed column
    if (sourceTable->indexed)
    {
        cout<<"TODO: Update index for the indexed column."<<endl;
    }
    
    // Print the inserted row
    cout << "Inserted row: ";
    for (int i = 0; i < values.size(); i++)
    {
        cout << values[i];
        if (i < values.size() - 1)
        {
            cout << ", ";
        }
    }
    cout << endl;
}