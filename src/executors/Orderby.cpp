#include "global.h"
#include "externalsort.h"

/**
 * @brief
 * SYNTAX: Result-table <- ORDER BY attribute-name ASC|DESC ON table-name
 */
bool syntacticParseORDERBY()
{
  logger.log("syntacticParseORDERBY");

  // Check if query has correct number of tokens
  if (tokenizedQuery.size() != 8 ||
      tokenizedQuery[1] != "<-" ||
      tokenizedQuery[2] != "ORDER" ||
      tokenizedQuery[3] != "BY" ||
      tokenizedQuery[6] != "ON")
  {
    cout << "SYNTAX ERROR: Invalid ORDER BY syntax" << endl;
    return false;
  }

  // Parse components of the query
  parsedQuery.queryType = ORDERBY;
  parsedQuery.resultRelationName = tokenizedQuery[0]; // Result table name
  parsedQuery.sortColumnNames = {tokenizedQuery[4]};  // column to sort by
  parsedQuery.sortRelationName = tokenizedQuery[7];   // Original table name

  // Determine sort direction
  // if (tokenizedQuery[4].find("DESC") != string::npos)
  // {
  //   // Remove "DESC" if present and set sorting direction
  //   parsedQuery.sortColumnNames[0] =
  //       parsedQuery.sortColumnNames[0].substr(0, parsedQuery.sortColumnNames[0].length() - 4);
  //   parsedQuery.sortingDirection = {false}; // Descending
  // }
  // else if (tokenizedQuery[4].find("ASC") != string::npos)
  // {
  //   // Remove "ASC" if present and set sorting direction
  //   parsedQuery.sortColumnNames[0] =
  //       parsedQuery.sortColumnNames[0].substr(0, parsedQuery.sortColumnNames[0].length() - 3);
  //   parsedQuery.sortingDirection = {true}; // Ascending
  // }
  // else
  // {
  //   // Default to ascending if no direction specified
  //   cout << "SYNTAX ERROR: Sorting Direction ASC|DESC is not specified" << endl;
  // }

  //  set sorting direction
  // cout << tokenizedQuery[5] << endl;
  if (tokenizedQuery[5] == "DESC")
  {
    parsedQuery.sortingDirection = {false}; // Descending
  }
  else if (tokenizedQuery[5]=="ASC")
  {
    parsedQuery.sortingDirection = {true}; // Ascending
  }
  else
  {
    //  If no direction specified
    cout << "SYNTAX ERROR: Sorting Direction ASC|DESC is not specified" << endl;
  }

  return true;
}

bool semanticParseORDERBY()
{
  logger.log("semanticParseORDERBY");

  // Check if source table exists
  if (!tableCatalogue.isTable(parsedQuery.sortRelationName))
  {
    cout << "SEMANTIC ERROR: Source table doesn't exist" << endl;
    return false;
  }

  // Check if result table name is not already in use
  if (tableCatalogue.isTable(parsedQuery.resultRelationName))
  {
    cout << "SEMANTIC ERROR: Result table already exists" << endl;
    return false;
  }

  // Retrieve the source table
  Table *table = tableCatalogue.getTable(parsedQuery.sortRelationName);

  // Check if the column exists in the table
  if (!table->isColumn(parsedQuery.sortColumnNames[0]))
  {
    cout << "SEMANTIC ERROR: Column " << parsedQuery.sortColumnNames[0]
         << " doesn't exist in table " << parsedQuery.sortRelationName << endl;
    return false;
  }

  return true;
}

void executeORDERBY()
{
  logger.log("executeORDERBY");

  // Retrieve the source table
  Table *table = tableCatalogue.getTable(parsedQuery.sortRelationName);

  // Create External Sort object
  ExternalSort externalSort(
      table,
      parsedQuery.sortColumnNames,
      parsedQuery.sortingDirection);

  // Perform Order By with result table name
  externalSort.performOrderBy(
      parsedQuery.resultRelationName,
      parsedQuery.sortColumnNames[0],
      parsedQuery.sortingDirection[0]);

  // Add the new table to the catalogue
  // tableCatalogue.insertTable(tableCatalogue.getTable(parsedQuery.resultRelationName));
}