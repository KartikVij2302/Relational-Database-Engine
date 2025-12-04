#include "global.h"
#include "math.h"
#include "externalsort.h"

/**
 * @brief
 * SYNTAX: SORT <table-name> BY <col1>, <col2>,<col3> IN <ASC|DESC>, <ASC|DESC>, <ASC|DESC>
 */

bool syntacticParseSORT()
{
  logger.log("syntacticParseSORT");

  if (tokenizedQuery.size() < 6)
  {
    cout << "SYNTAX ERROR: Query size < 6" << endl;
    return false;
  }

  if (tokenizedQuery[2] != "BY" || tokenizedQuery.size() <= 4 ||
      find(tokenizedQuery.begin(), tokenizedQuery.end(), "IN") == tokenizedQuery.end())
  {
    cout << "SYNTAX ERROR: Query order is wrong" << endl;
    return false;
  }

  parsedQuery.queryType = SORT;
  parsedQuery.sortRelationName = tokenizedQuery[1];

  // Find position of "IN"
  auto inPos = find(tokenizedQuery.begin(), tokenizedQuery.end(), "IN");
  int inIndex = distance(tokenizedQuery.begin(), inPos);

  // Parse columns (between BY and IN)
  vector<string> columnsList;
  for (int i = 3; i < inIndex; i++)
  {
    columnsList.push_back(tokenizedQuery[i]); // Columns are space-separated
  }

  // Parse sorting directions (after IN)
  vector<string> directionsList;
  for (size_t i = inIndex + 1; i < tokenizedQuery.size(); i++)
  {
    directionsList.push_back(tokenizedQuery[i]); // Directions are space-separated
  }

  // Check if the number of columns matches the number of directions
  if (columnsList.size() != directionsList.size())
  {
    cout << "SYNTAX ERROR: Number of columns does not match number of sorting directions" << endl;
    return false;
  }

  parsedQuery.sortColumnNames = columnsList;

  // Validate and store sorting directions
  parsedQuery.sortingDirection.clear();
  for (const string &direction : directionsList)
  {
    if (direction == "ASC")
    {
      parsedQuery.sortingDirection.push_back(true); // true for ascending
    }
    else if (direction == "DESC")
    {
      parsedQuery.sortingDirection.push_back(false); // false for descending
    }
    else
    {
      cout << "SYNTAX ERROR: Invalid sorting direction. Use ASC or DESC" << endl;
      return false;
    }
  }

  return true;
}

bool semanticParseSORT()
{
  logger.log("semanticParseSORT");

  // Check if table exists
  // cout << "sortRelationName: " << parsedQuery.sortRelationName << endl;

  if (!tableCatalogue.isTable(parsedQuery.sortRelationName))
  {
    cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
    return false;
  }

  Table *table = tableCatalogue.getTable(parsedQuery.sortRelationName);

  // Check if the columns exist in the table
  for (const string &columnName : parsedQuery.sortColumnNames)
  {
    if (!table->isColumn(columnName))
    {
      cout << "SEMANTIC ERROR: Column " << columnName << " doesn't exist in relation " << parsedQuery.sortRelationName << endl;
      return false;
    }
  }

  return true;
}












// Hi
void executeSORT()
{
  logger.log("executeSORT");

  // Retrieve the table
  Table *table = tableCatalogue.getTable(parsedQuery.sortRelationName);

  // Create External Sort object
  ExternalSort externalSort(
      table,
      parsedQuery.sortColumnNames,
      parsedQuery.sortingDirection);

  // Perform external sort
  externalSort.performExternalSort();
}