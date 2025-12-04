#include "global.h"
#include "secondary_index.h"

/**
 * @brief Syntax: DELETE FROM <table_name> WHERE <column_name> <operator> <value>
 *
 * This query deletes rows from a table where the specified column satisfies the condition.
 * It uses secondary indices when available for efficiency.
 */
bool syntacticParseDELETE()
{
  logger.log("syntacticParseDELETE");

  // Check if the query has the correct format
  if (tokenizedQuery.size() != 7 || tokenizedQuery[0] != "DELETE" || tokenizedQuery[1] != "FROM" || tokenizedQuery[3] != "WHERE")
  {
    cout << "SYNTAX ERROR" << endl;
    return false;
  }

  // Extract the parameters
  parsedQuery.queryType = DELETE;
  parsedQuery.deleteRelationName = tokenizedQuery[2];
  parsedQuery.deleteColumnName = tokenizedQuery[4];

  // Parse the operator
  string op = tokenizedQuery[5];
  if (op == "<")
    parsedQuery.deleteOperator = "<";
  else if (op == "<=")
    parsedQuery.deleteOperator = "<=";
  else if (op == ">")
    parsedQuery.deleteOperator = ">";
  else if (op == ">=")
    parsedQuery.deleteOperator = ">=";
  else if (op == "==")
    parsedQuery.deleteOperator = "==";
  else if (op == "!=")
    parsedQuery.deleteOperator = "!=";
  else
  {
    cout << "SYNTAX ERROR: Invalid operator" << endl;
    return false;
  }

  // The last token is the value
  parsedQuery.deleteValue = stoi(tokenizedQuery[6]);

  return true;
}

/**
 * @brief Semantic validation for the DELETE query
 */
bool semanticParseDELETE()
{
  logger.log("semanticParseDELETE");

  // Check if the table exists
  if (!tableCatalogue.isTable(parsedQuery.deleteRelationName))
  {
    cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
    return false;
  }

  // Check if the column exists in the table
  Table *table = tableCatalogue.getTable(parsedQuery.deleteRelationName);
  if (!table->isColumn(parsedQuery.deleteColumnName))
  {
    cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
    return false;
  }

  // Check if a secondary index file exists for this column
  string indexFileName = "../data/indices/" + parsedQuery.deleteRelationName + "_" +
                         parsedQuery.deleteColumnName + "_Indexfile_0";
  ifstream indexFile(indexFileName);
  if (!indexFile)
  {
    logger.log("No secondary index found for " + parsedQuery.deleteRelationName + "." +
               parsedQuery.deleteColumnName + ". Will perform linear scan.");
  }
  else
  {
    indexFile.close();
    logger.log("Secondary index found for " + parsedQuery.deleteRelationName + "." +
               parsedQuery.deleteColumnName + ". Will use indexed search.");
  }

  return true;
}





















//

/**
 * @brief Execute the DELETE query
 *
 * This function deletes records from a table that satisfy the given condition.
 * It uses secondary indices when available for efficiency, otherwise falls back to a linear scan.
 * The implementation creates a new table with non-matching records, replaces the original table,
 * and updates any secondary indices.
 */
void executeDELETE()
{
  logger.log("executeDELETE");

  // Extract delete parameters from parsed query
  string tableName = parsedQuery.deleteRelationName;
  string columnName = parsedQuery.deleteColumnName;
  string deleteOperator = parsedQuery.deleteOperator;
  int deleteValue = parsedQuery.deleteValue;

  // Get the table to delete from
  Table *sourceTable = tableCatalogue.getTable(tableName);
  if (!sourceTable)
  {
    cout << "ERROR: Table not found" << endl;
    return;
  }

  // Get column index
  int columnIndex = sourceTable->getColumnIndex(columnName);
  if (columnIndex == -1)
  {
    cout << "ERROR: Column not found in table" << endl;
    return;
  }

  // Negate the operator for finding records to keep
  string negatedOperator;
  if (deleteOperator == "<")
    negatedOperator = ">=";
  else if (deleteOperator == "<=")
    negatedOperator = ">";
  else if (deleteOperator == ">")
    negatedOperator = "<=";
  else if (deleteOperator == ">=")
    negatedOperator = "<";
  else if (deleteOperator == "==")
    negatedOperator = "!=";
  else if (deleteOperator == "!=")
    negatedOperator = "==";

  logger.log("DELETE: Finding records where " + columnName + " " + deleteOperator + " " + to_string(deleteValue));
  logger.log("DELETE: Keeping records where " + columnName + " " + negatedOperator + " " + to_string(deleteValue));

  // Check if secondary index exists for this column
  SecondaryIndex *indexObj = nullptr;
  string indexFileName = "../data/indices/" + tableName + "_" + columnName + "_Indexfile_0";
  bool indexExists = false;
  ifstream indexFile(indexFileName);

  if (indexFile)
  {
    indexFile.close();
    indexExists = true;
    logger.log("Using existing secondary index for " + tableName + "." + columnName);
    indexObj = new SecondaryIndex(tableName, columnName);
    indexObj->readIndex();
  }
  else
  {
    logger.log("No secondary index found for " + tableName + "." + columnName + ". Using linear scan.");
  }

  // Create a temporary table with the same schema as the source table
  string tempTableName = tableName + "_temp_delete";
  vector<string> columns = sourceTable->getColumnNames();
  Table *tempTable = new Table(tempTableName, columns);

  int recordsKept = 0;
  int recordsDeleted = 0;

  if (indexExists && indexObj)
  {
    // Use index-based approach - find matching BP files based on negated search operation
    vector<string> matchingBPFiles = indexObj->findMatchingBPFiles(negatedOperator, deleteValue);
    string indexDir = "../data/indices/";

    // Process each BP file to extract records to keep
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
        Page page = bufferManager.getPage(tableName, pageNo);
        vector<int> row = page.getRow(recordNo);

        // Add row to temporary table
        tempTable->writeRow<int>(row);
        recordsKept++;
      }

      bpFile.close();
    }

    // Calculate how many records were deleted
    recordsDeleted = sourceTable->rowCount - recordsKept;
  }
  else
  {
    // Linear scan approach if no index is available
    Cursor cursor = sourceTable->getCursor();
    vector<int> row = cursor.getNext();

    while (!row.empty())
    {
      // Check if this row should be kept (not deleted)
      bool keepRow = false;

      if (negatedOperator == "<")
        keepRow = row[columnIndex] < deleteValue;
      else if (negatedOperator == "<=")
        keepRow = row[columnIndex] <= deleteValue;
      else if (negatedOperator == ">")
        keepRow = row[columnIndex] > deleteValue;
      else if (negatedOperator == ">=")
        keepRow = row[columnIndex] >= deleteValue;
      else if (negatedOperator == "==")
        keepRow = row[columnIndex] == deleteValue;
      else if (negatedOperator == "!=")
        keepRow = row[columnIndex] != deleteValue;

      if (keepRow)
      {
        // Add row to temporary table
        tempTable->writeRow<int>(row);
        recordsKept++;
      }
      else
      {
        recordsDeleted++;
      }

      // Get next row
      row = cursor.getNext();
    }
  }













  // Finalize the temporary table
  // Finalize the temp table
  tempTable->blockify();
  tableCatalogue.insertTable(tempTable);

  // Now replace the original table with the temp table
  // First, drop the original table
  tableCatalogue.deleteTable(tableName);

  // Then rename the temp table to the original name
  Table *newTable = new Table(tableName, columns);

  // Copy data from temp table to new table
  Cursor tempCursor = tempTable->getCursor();
  vector<int> tempRow;
  while (true)
  {
    tempRow = tempCursor.getNext();
    if (tempRow.empty())
      break;
    newTable->writeRow<int>(tempRow);
  }

  // Finalize the new table
  newTable->blockify();
  tableCatalogue.insertTable(newTable);

  // Delete the temp table
  tableCatalogue.deleteTable(tempTableName);

  SecondaryIndex newIndex(tableName, columnName);
  newIndex.createIndex();

  bufferManager.clearPool();

  // Print summary
  if (recordsDeleted > 0)
  {
    cout << "Deleted " << recordsDeleted << " rows from table '" << tableName << "'." << endl;
  }
  else
  {
    cout << "No rows matched the deletion condition." << endl;
  }
}