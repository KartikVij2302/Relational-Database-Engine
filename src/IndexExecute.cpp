#include "global.h"
#include "index_manager.h"
/**
 * @brief
 * SYNTAX: INDEX ON table_name USING column_name
 */
bool syntacticParseINDEX()
{
  logger.log("syntacticParseINDEX");
  // cout << "[DEBUG]" << ("syntacticParseINDEX") << endl;

  if (tokenizedQuery.size() != 5 || tokenizedQuery[1] != "ON" || tokenizedQuery[3] != "USING")
  {
    cout << "SYNTAX ERROR: Correct syntax: INDEX ON table_name USING column_name" << endl;
    return false;
  }

  parsedQuery.queryType = INDEX;
  parsedQuery.indexRelationName = tokenizedQuery[2];
  parsedQuery.indexColumnName = tokenizedQuery[4];

  return true;
}

bool semanticParseINDEX()
{
  logger.log("semanticParseINDEX");
  // cout << "[DEBUG]" << ("syntacticParseINDEX") << endl;

  if (!tableCatalogue.isTable(parsedQuery.indexRelationName))
  {
    cout << "SEMANTIC ERROR: Table " << parsedQuery.indexRelationName << " does not exist" << endl;
    return false;
  }

  Table *table = tableCatalogue.getTable(parsedQuery.indexRelationName);
  if (!table->isColumn(parsedQuery.indexColumnName))
  {
    cout << "SEMANTIC ERROR: Column " << parsedQuery.indexColumnName << " does not exist in table " << parsedQuery.indexRelationName << endl;
    return false;
  }

  return true;
}

void executeINDEX()
{
  logger.log("executeINDEX");
  // cout<< "[DEBUG]" <<("executeINDEX")<<endl;

  if (indexManager.createIndex(parsedQuery.indexRelationName, parsedQuery.indexColumnName))
  {
    cout << "Index created on " << parsedQuery.indexRelationName << "." << parsedQuery.indexColumnName << endl;
  }
  else
  {
    cout << "Failed to create index on " << parsedQuery.indexRelationName << "." << parsedQuery.indexColumnName << endl;
  }

  // SecondaryIndex *index = new SecondaryIndex(tableName, columnName);
  // if (indexManager.createIndex(parsedQuery.indexRelationName, parsedQuery.indexColumnName))
  // {
  //   cout << "Index created on " << parsedQuery.indexRelationName << "." << parsedQuery.indexColumnName << endl;
  // }
  // else
  // {
  //   cout << "Failed to create index on " << parsedQuery.indexRelationName << "." << parsedQuery.indexColumnName << endl;
  // }

  // if (!index->createIndex())
  // {
  //   logger.log("IndexManager::createIndex: Failed to create index");
  //   delete index;
  //   return false;
  // }
}