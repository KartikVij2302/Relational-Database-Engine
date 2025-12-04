#ifndef INDEX_MANAGER_H
#define INDEX_MANAGER_H

#include "global.h"
#include "secondary_index.h"

/**
 * @brief Class to manage secondary indices in the system
 */
class IndexManager
{
private:
  // Map of table_column -> SecondaryIndex
  map<string, SecondaryIndex *> indices;

  // Helper to get index key from table and column names
  string getIndexKey(string tableName, string columnName);

public:
  /**
   * @brief Constructor
   */
  IndexManager();

  /**
   * @brief Destructor
   */
  ~IndexManager();

  /**
   * @brief Create an index on a table column
   *
   * @param tableName Name of the table
   * @param columnName Name of the column
   * @return true if created successfully or already exists
   * @return false if failed to create
   */
  bool createIndex(string tableName, string columnName);

  /**
   * @brief Check if an index exists on a column
   *
   * @param tableName Name of the table
   * @param columnName Name of the column
   * @return true if index exists
   * @return false otherwise
   */
  bool hasIndex(string tableName, string columnName);

  /**
   * @brief Get an index
   *
   * @param tableName Name of the table
   * @param columnName Name of the column
   * @return SecondaryIndex* Pointer to the index or nullptr if not found
   */
  SecondaryIndex *getIndex(string tableName, string columnName);

  /**
   * @brief Search for records using an index
   *
   * @param tableName Name of the table
   * @param columnName Name of the column
   * @param value Value to search for
   * @return vector<pair<int, int>> Vector of (pageIndex, rowIndex) pairs or empty if index not found
   */
  vector<pair<int, int>> search(string tableName, string columnName, int value);

  /**
   * @brief Search for records within a range using an index
   *
   * @param tableName Name of the table
   * @param columnName Name of the column
   * @param lowerBound Lower bound (inclusive)
   * @param upperBound Upper bound (inclusive)
   * @return vector<pair<int, int>> Vector of (pageIndex, rowIndex) pairs or empty if index not found
   */
  vector<pair<int, int>> rangeSearch(string tableName, string columnName, int lowerBound, int upperBound);
};

// Global instance of index manager
extern IndexManager indexManager;

#endif // INDEX_MANAGER_H