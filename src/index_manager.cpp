#include "index_manager.h"

// Initialize global index manager
IndexManager indexManager;

IndexManager::IndexManager()
{
  // Nothing to initialize
}

IndexManager::~IndexManager()
{
  // Clean up all indices
  for (auto &pair : indices)
  {
    delete pair.second;
  }
  indices.clear();
}

string IndexManager::getIndexKey(string tableName, string columnName)
{
  return tableName + "_" + columnName;
}

bool IndexManager::createIndex(string tableName, string columnName)
{
  logger.log("IndexManager::createIndex on " + tableName + "." + columnName);

  // Check if table exists
  if (!tableCatalogue.isTable(tableName))
  {
    logger.log("IndexManager::createIndex: Table does not exist");
    return false;
  }

  // Check if column exists
  Table *table = tableCatalogue.getTable(tableName);
  if (!table->isColumn(columnName))
  {
    logger.log("IndexManager::createIndex: Column does not exist in table");
    return false;
  }

  // Check if index already exists
  string indexKey = getIndexKey(tableName, columnName);
  if (indices.find(indexKey) != indices.end())
  {
    logger.log("IndexManager::createIndex: Index already exists");
    return true; // Already exists, so technically successful
  }

  // Create the index
  cout << "[DEBUG]" << ("Creating Index..") << endl;
  SecondaryIndex *index = new SecondaryIndex(tableName, columnName);
  if (!index->createIndex())
  {
    logger.log("IndexManager::createIndex: Failed to create index");
    delete index;
    return false;
  }
  cout << "[DEBUG]" << ("Created Index") << endl;

  // cout << "[DEBUG]" << ("Storeing the index..") << endl;
  // Store the index
  indices[indexKey] = index;

  logger.log("IndexManager::createIndex: Index created successfully");
  return true;
}




bool IndexManager::hasIndex(string tableName, string columnName)
{
  string indexKey = getIndexKey(tableName, columnName);
  return indices.find(indexKey) != indices.end();
}

SecondaryIndex *IndexManager::getIndex(string tableName, string columnName)
{
  string indexKey = getIndexKey(tableName, columnName);

  if (indices.find(indexKey) != indices.end())
  {
    return indices[indexKey];
  }

  // Check if the index exists on disk but not loaded
  string indexFileName = "../data/indices/" + tableName + "_" + columnName + "_index.meta";
  ifstream testFile(indexFileName);
  if (testFile.good())
  {
    testFile.close();

    // Create and load the index
    SecondaryIndex *index = new SecondaryIndex(tableName, columnName);
    indices[indexKey] = index;
    return index;
  }

  return nullptr;
}

// vector<pair<int, int>> IndexManager::search(string tableName, string columnName, int value)
// {
//   SecondaryIndex *index = getIndex(tableName, columnName);

//   if (index)
//   {
//     return index->search(value);
//   }

//   return {}; // Empty result if no index
// }

// vector<pair<int, int>> IndexManager::rangeSearch(string tableName, string columnName, int lowerBound, int upperBound)
// {
//   SecondaryIndex *index = getIndex(tableName, columnName);

//   if (index)
//   {
//     return index->rangeSearch(lowerBound, upperBound);
//   }

//   return {}; // Empty result if no index
// }