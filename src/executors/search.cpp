#include "global.h"
#include "secondary_index.h"
/**
 * @brief Syntax: <new_table> <- SEARCH FROM <table_name> WHERE <column_name> <operator> <value>
 *
 * This query searches for rows in a table where the specified column satisfies the condition
 * given by the operator and value. It uses a secondary index for efficient searching if available.
 */
bool syntacticParseSEARCH()
{
  logger.log("syntacticParseSEARCH");

  // Check if the query has the correct format
  if (tokenizedQuery.size() != 9 || tokenizedQuery[1] != "<-" || tokenizedQuery[3] != "FROM" || tokenizedQuery[5] != "WHERE")
  {
    cout << "SYNTAX ERROR" << endl;
    return false;
  }

  // Extract the search parameters
  parsedQuery.queryType = SEARCH;
  parsedQuery.searchResultRelationName = tokenizedQuery[0];
  parsedQuery.searchRelationName = tokenizedQuery[4];
  parsedQuery.searchColumnName = tokenizedQuery[6];

  // Parse the operator
  string op = tokenizedQuery[7];
  if (op == "<")
    parsedQuery.searchOperator = "<";
  else if (op == "<=")
    parsedQuery.searchOperator = "<=";
  else if (op == ">")
    parsedQuery.searchOperator = ">";
  else if (op == ">=")
    parsedQuery.searchOperator = ">=";
  else if (op == "==")
    parsedQuery.searchOperator = "==";
  else if (op == "!=")
    parsedQuery.searchOperator = "!=";
  else
  {
    cout << "SYNTAX ERROR: Invalid operator" << endl;
    return false;
  }

  // The last token is the value
  parsedQuery.searchValue = stoi(tokenizedQuery[8]);

  return true;
}

/**
 * @brief Semantic validation for the SEARCH query
 */
bool semanticParseSEARCH()
{
  logger.log("semanticParseSEARCH");

  // Check if the source table exists
  if (!tableCatalogue.isTable(parsedQuery.searchRelationName))
  {
    cout << "SEMANTIC ERROR: Source relation doesn't exist" << endl;
    return false;
  }

  // Check if the result table doesn't already exist
  if (tableCatalogue.isTable(parsedQuery.searchResultRelationName))
  {
    cout << "SEMANTIC ERROR: Result relation already exists" << endl;
    return false;
  }

  // Check if the column exists in the source table
  Table *sourceTable = tableCatalogue.getTable(parsedQuery.searchRelationName);
  if (!sourceTable->isColumn(parsedQuery.searchColumnName))
  {
    cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
    return false;
  }

  // Additionally, check if a secondary index file exists for this column
  string indexFileName = "../data/indices/" + parsedQuery.searchRelationName + "_" +
                         parsedQuery.searchColumnName + "_Indexfile_0";
  ifstream indexFile(indexFileName);
  if (!indexFile)
  {
    logger.log("No secondary index found for " + parsedQuery.searchRelationName + "." +
               parsedQuery.searchColumnName + ". Will perform linear scan.");
  }
  else
  {
    indexFile.close();
    logger.log("Secondary index found for " + parsedQuery.searchRelationName + "." +
               parsedQuery.searchColumnName + ". Will use indexed search.");
  }

  return true;
}



void executeSEARCH()
{
  logger.log("executeSEARCH");

  // Extract search parameters from parsed query
  string sourceRelation = parsedQuery.searchRelationName;
  string resultRelation = parsedQuery.searchResultRelationName;
  string columnName = parsedQuery.searchColumnName;
  string searchOperator = parsedQuery.searchOperator;
  int searchValue = parsedQuery.searchValue;

  // Get source table
  Table *sourceTable = tableCatalogue.getTable(sourceRelation);
  if (!sourceTable)
  {
    cout << "ERROR: Source table not found" << endl;
    return;
  }

  // Get column index
  int columnIndex = sourceTable->getColumnIndex(columnName);
  if (columnIndex == -1)
  {
    cout << "ERROR: Column not found in table" << endl;
    return;
  }

  // Check if secondary index exists for this column, create if not
  SecondaryIndex *indexObj = nullptr;
  string indexFileName = "../data/indices/" + sourceRelation + "_" + columnName + "_Indexfile_0";
  ifstream indexFile(indexFileName);

  if (!indexFile)
  {
    logger.log("Creating secondary index for " + sourceRelation + "." + columnName);
    indexObj = new SecondaryIndex(sourceRelation, columnName);
    if (!indexObj->createIndex())
    {
      cout << "ERROR: Failed to create secondary index" << endl;
      delete indexObj;
      return;
    }
  }
  else
  {
    indexFile.close();
    logger.log("Using existing secondary index for " + sourceRelation + "." + columnName);
    indexObj = new SecondaryIndex(sourceRelation, columnName);
    indexObj->readIndex();
  }

  // Create result table with same schema as source table
  vector<string> columns = sourceTable->getColumnNames();
  Table *resultTable = new Table(resultRelation, columns);

  // Find matching BP files based on search operation
  vector<string> matchingBPFiles = indexObj->findMatchingBPFiles(searchOperator, searchValue);

  // Process each BP file to extract matching records
  string indexDir = "../data/indices/";
  int matchingRowsCount = 0;

  for (const string &bpFileName : matchingBPFiles)
  {
    string bpFilePath = indexDir + bpFileName;
    ifstream bpFile(bpFilePath);

    if (!bpFile)
    {
      logger.log("Warning: Could not open BP file: " + bpFilePath);
      continue;
    }

    // Read record count
    int recordCount;
    bpFile >> recordCount;

    // Process each record pointer
    for (int i = 0; i < recordCount; i++)
    {
      int pageNo, recordNo;
      bpFile >> pageNo >> recordNo;

      // Get the actual row from the source table
      Page page = bufferManager.getPage(sourceRelation, pageNo);
      vector<int> row = page.getRow(recordNo);

      // Add row to result table
      resultTable->writeRow<int>(row);
      matchingRowsCount++;
    }

    bpFile.close();
  }

  // Finalize the result table
  resultTable->blockify();
  tableCatalogue.insertTable(resultTable);

  cout << "SEARCH RESULT: " << matchingRowsCount << " rows matching the condition." << endl;

  // Clean up
  delete indexObj;
}