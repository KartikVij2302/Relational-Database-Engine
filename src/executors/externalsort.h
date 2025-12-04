#ifndef EXTERNAL_SORT_H
#define EXTERNAL_SORT_H

#include "global.h"
#include <queue>
#include <vector>
#include <algorithm>
#include <fstream>

// Maximum number of buffer blocks for external sorting
const int EXTERNAL_SORT_BUFFER_BLOCKS = 10;

class ExternalSort
{
private:
  // Reference to the original table
  Table *table;

  // Columns to sort by
  vector<string> sortColumns;

  // Sorting directions (true for ascending, false for descending)
  vector<bool> sortDirections;

  // Generate a unique temporary filename
  string generateTempFileName(int runNumber);

  // Sort a single run (chunk) of data
  void sortRun(vector<vector<int>> &run);

  // Merge sorted runs
  void mergeRuns(vector<string> &runFiles);

  // Custom comparator for sorting
  bool compareRows(const vector<int> &a, const vector<int> &b);

  // Helper function to read a block from a file
  vector<vector<int>> readBlockFromFile(ifstream &file);

  // Helper function to write a block to a file
  void writeBlockToFile(ofstream &file, const vector<vector<int>> &block);

public:
  ExternalSort(Table *table,
               const vector<string> &sortColumns,
               const vector<bool> &sortDirections);

  // Main external sort method
  void performExternalSort();

  void performOrderBy(const string &resultTableName,
                      const string &sortColumn,
                      bool isAscending);

  // Clean up temporary files
  void cleanupTempFiles(const vector<string> &tempFiles);
};

#endif // EXTERNAL_SORT_H