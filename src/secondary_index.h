#ifndef SECONDARY_INDEX_H
#define SECONDARY_INDEX_H

#include "global.h"

// Constants for file sizes
const int MAX_DISTINCT_VALUES_PER_INDEX_BLOCK = 100; // Max distinct values per index file
const int MAX_RECORDS_PER_BP_BLOCK = 1000;           // Max record pointers per block pointer file


/**
 * @brief Structure to represent an index entry with field value and metadata
 */
struct IndexEntry {
  int fieldValue;
  vector<string> blockPointers; // Names of block pointer files
  int blockCount;
  int recordCount;
};

/**
 * @brief Class to manage the secondary index structure with memory constraints
 */
class SecondaryIndex
{
private:
  string tableName;
  string columnName;
  int columnIndex;

  vector<IndexEntry> indexEntries; // Array of index entries

  /**
   * @brief Binary search to find the index entry for a given field value
   *
   * @param fieldValue Value to search for
   * @return int Index of the entry if found, -1 otherwise
   */
  int binarySearch(int fieldValue);





public:
  /**
   * @brief Read index from disk
   */
  void readIndex();
  /**
   * @brief Construct a new Secondary Index object
   *
   * @param tableName Name of the table being indexed
   * @param columnName Name of the column being indexed
   */
  SecondaryIndex(string tableName, string columnName);

  /**
   * @brief Create the index on the specified column
   * Uses memory-efficient approach with max 10 blocks in memory
   *
   * @return true if index created successfully
   * @return false otherwise
   */
  bool createIndex();

  /**
   * @brief Search for records with a specific value
   *
   * @param value The value to search for
   * @return vector<pair<int, int>> Vector of (pageIndex, rowIndex) pairs
   */
  vector<pair<int, int>> search(int value);

  /**
   * @brief Search for records within a range of values
   *
   * @param lowerBound Lower bound value (inclusive)
   * @param upperBound Upper bound value (inclusive)
   * @return vector<pair<int, int>> Vector of (pageIndex, rowIndex) pairs
   */
  vector<pair<int, int>> rangeSearch(int lowerBound, int upperBound);

  /**
   * @brief Get the table name
   *
   * @return string The table name
   */
  string getTableName() const;

  /**
   * @brief Get the column name
   *
   * @return string The column name
   */
  string getColumnName() const;
  bool updateIndex(int newValue);


  /*
  * Finds the index file that might contain the given value
  * Returns file path if found, empty string if not found
  */
    string findIndexFileForValue(int value);

  /*
    * Performs binary search in an index file to find a specific value
    * Returns BP file names if found, empty vector if not found
    */
  vector<string> binarySearchInIndexFile(const string &indexFilePath, int targetValue);
  /*
    * Finds all values in the index that match the operator and search value
    * Returns vector of BP file names that contain matching records
    */
  vector<string> findMatchingBPFiles(const string &searchOperator, int searchValue);
};

#endif // SECONDARY_INDEX_H