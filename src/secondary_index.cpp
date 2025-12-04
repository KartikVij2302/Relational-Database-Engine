#include "secondary_index.h"

SecondaryIndex::SecondaryIndex(string tableName, string columnName)
{
  this->tableName = tableName;
  this->columnName = columnName;

  // Get the column index from the table
  Table *table = tableCatalogue.getTable(tableName);
  if (table)
  {
    this->columnIndex = table->getColumnIndex(columnName);
  }
  else
  {
    logger.log("SecondaryIndex: Table not found");
    this->columnIndex = -1;
  }

  // Load the index
  readIndex();
}

int SecondaryIndex::binarySearch(int fieldValue)
{
  int left = 0;
  int right = indexEntries.size() - 1;

  while (left <= right)
  {
    int mid = left + (right - left) / 2;

    if (indexEntries[mid].fieldValue == fieldValue)
      return mid;

    if (indexEntries[mid].fieldValue < fieldValue)
      left = mid + 1;
    else
      right = mid - 1;
  }

  return -1; // Not found
}

void SecondaryIndex::readIndex()
{
  // Clear existing entries
  indexEntries.clear();

  // First, determine how many index blocks exist
  int blockNo = 0;
  bool moreBlocks = true;

  while (moreBlocks)
  {
    string indexFileName = "../data/indices/" + tableName + "_" + columnName + "_Indexfile_" + to_string(blockNo);
    ifstream indexFile(indexFileName);

    if (!indexFile)
    {
      if (blockNo == 0)
      {
        logger.log("SecondaryIndex::readIndex: No index files found");
      }
      moreBlocks = false;
      continue;
    }

    // Read number of distinct values in this block
    int distinctCount;
    indexFile >> distinctCount;

    // Read each distinct value and its block pointer list
    for (int i = 0; i < distinctCount; i++)
    {
      IndexEntry entry;
      indexFile >> entry.fieldValue;

      // Read comma-separated list of block pointers
      string blockPointerList;
      indexFile >> blockPointerList;

      // Parse the comma-separated list
      stringstream ss(blockPointerList);
      string blockPointerName;
      vector<string> blockPointers;

      while (getline(ss, blockPointerName, ','))
      {
        blockPointers.push_back(blockPointerName);
      }

      entry.blockPointers = blockPointers;
      entry.blockCount = blockPointers.size();

      // Count total records by examining first block pointer
      entry.recordCount = 0;
      if (!blockPointers.empty())
      {
        string firstBPFile = "../data/indices/" + blockPointers[0];
        ifstream bpFile(firstBPFile);
        if (bpFile)
        {
          int count;
          bpFile >> count;
          entry.recordCount = count;
          bpFile.close();
        }
      }

      indexEntries.push_back(entry);
    }

    indexFile.close();
    blockNo++;
  }

  logger.log("SecondaryIndex::readIndex: Loaded " + to_string(indexEntries.size()) + " index entries from " + to_string(blockNo) + " blocks");

  // Sort indexEntries for binary search
  sort(indexEntries.begin(), indexEntries.end(), [](const IndexEntry &a, const IndexEntry &b)
       { return a.fieldValue < b.fieldValue; });
}




bool SecondaryIndex::createIndex()
{
  logger.log("SecondaryIndex::createIndex");

  if (columnIndex == -1)
  {
    logger.log("SecondaryIndex::createIndex: Invalid column index");
    return false;
  }

  Table *table = tableCatalogue.getTable(tableName);
  if (!table)
  {
    logger.log("SecondaryIndex::createIndex: Table not found");
    return false;
  }

  // Create index directory if it doesn't exist
  string indexDir = "../data/indices/";
  if (!filesystem::exists(indexDir))
  {
    filesystem::create_directories(indexDir);
  }

  // Constants for file sizes
  const int MAX_DISTINCT_VALUES_PER_INDEX_BLOCK = 100; // Max distinct values per index file
  const int MAX_RECORDS_PER_BP_BLOCK = 1000;           // Max record pointers per block pointer file

  // First phase: Sort the column values and identify unique values
  // We'll use a temporary file approach to handle large datasets without loading everything into memory

  // Create temporary directory for intermediate processing
  string tempDir = "../data/temp/";
  if (!filesystem::exists(tempDir))
  {
    filesystem::create_directories(tempDir);
  }

  // Step 1: Scan the table once to extract column values and their locations
  logger.log("SecondaryIndex::createIndex: Phase 1 - Extracting column values");

  // Use multiple temporary files to store batches of (value, pageNo, recordNo)
  vector<string> tempFileNames;
  const int BATCH_SIZE = 10000; // Process in batches to manage memory

  vector<tuple<int, int, int>> currentBatch; // (value, pageNo, recordNo)
  int batchCount = 0;
  int totalRecords = 0;

  // Get a cursor to scan the table
  Cursor cursor = table->getCursor();
  vector<int> row = cursor.getNext();
  int pageNo = 0;
  int recordNo = 0;

  while (!row.empty())
  {
    // Get the value from the indexed column
    int fieldValue = row[columnIndex];

    // Add to current batch
    currentBatch.push_back(make_tuple(fieldValue, pageNo, recordNo));

    // If batch is full, sort and write to temp file
    if (currentBatch.size() >= BATCH_SIZE)
    {
      // Sort batch by value
      sort(currentBatch.begin(), currentBatch.end());

      // Write sorted batch to temp file
      string tempFileName = tempDir + tableName + "_" + columnName + "_temp_" + to_string(batchCount);
      ofstream tempFile(tempFileName);

      if (!tempFile)
      {
        logger.log("SecondaryIndex::createIndex: Failed to create temp file " + tempFileName);
        return false;
      }

      for (const auto &[value, page, record] : currentBatch)
      {
        tempFile << value << " " << page << " " << record << endl;
      }

      tempFile.close();
      tempFileNames.push_back(tempFileName);

      // Clear batch for next round
      currentBatch.clear();
      batchCount++;
    }

    // Get next row
    row = cursor.getNext();

    // Update page and record position tracking
    recordNo++;
    totalRecords++;

    // Logic to track page boundaries - adjust as needed based on your cursor implementation
    if (recordNo >= table->maxRowsPerBlock)
    {
      pageNo++;
      recordNo = 0;
    }
  }

  // Write any remaining records to a final temp file
  if (!currentBatch.empty())
  {
    // Sort batch by value
    sort(currentBatch.begin(), currentBatch.end());

    // Write sorted batch to temp file
    string tempFileName = tempDir + tableName + "_" + columnName + "_temp_" + to_string(batchCount);
    ofstream tempFile(tempFileName);

    if (!tempFile)
    {
      logger.log("SecondaryIndex::createIndex: Failed to create temp file " + tempFileName);
      return false;
    }

    for (const auto &[value, page, record] : currentBatch)
    {
      tempFile << value << " " << page << " " << record << endl;
    }

    tempFile.close();
    tempFileNames.push_back(tempFileName);
    currentBatch.clear();
  }

  logger.log("SecondaryIndex::createIndex: Created " + to_string(tempFileNames.size()) + " temporary sorted files");

  // Step 2: Merge the sorted temporary files and create the index structure
  logger.log("SecondaryIndex::createIndex: Phase 2 - Creating index files");

  // Open all temp files
  vector<ifstream> tempFiles;
  vector<tuple<int, int, int, int>> currentValues; // (value, pageNo, recordNo, fileIndex)

  for (int i = 0; i < tempFileNames.size(); i++)
  {
    tempFiles.push_back(ifstream(tempFileNames[i]));
    if (!tempFiles.back())
    {
      logger.log("SecondaryIndex::createIndex: Failed to open temp file " + tempFileNames[i]);
      return false;
    }

    // Read the first value from each file
    int value, page, record;
    if (tempFiles[i] >> value >> page >> record)
    {
      currentValues.push_back(make_tuple(value, page, record, i));
    }
  }

  // Create a min-heap for efficient merging
  auto compareValues = [](const tuple<int, int, int, int> &a, const tuple<int, int, int, int> &b)
  {
    return get<0>(a) > get<0>(b); // Min heap by value
  };

  make_heap(currentValues.begin(), currentValues.end(), compareValues);

  // Variables for index file creation
  int indexBlockCount = 0;
  int distinctValuesInCurrentBlock = 0;
  int currentValue = -1;
  vector<pair<int, int>> currentValueRecords; // (pageNo, recordNo) for current value
  map<int, vector<string>> blockPointerFiles; // value -> list of BP file names
  int totalDistinctValues = 0;

  // Create first index file
  string currentIndexFileName = indexDir + tableName + "_" + columnName + "_Indexfile_" + to_string(indexBlockCount);
  ofstream currentIndexFile(currentIndexFileName);
  if (!currentIndexFile)
  {
    logger.log("SecondaryIndex::createIndex: Failed to create index file " + currentIndexFileName);
    return false;
  }

  // Reserve space for count at the beginning
  currentIndexFile << "          " << endl; // Placeholder for distinct count

  // Process all values in sorted order
  while (!currentValues.empty())
  {
    // Get the smallest value
    pop_heap(currentValues.begin(), currentValues.end(), compareValues);
    auto [value, page, record, fileIndex] = currentValues.back();
    currentValues.pop_back();

    // Read next value from the file
    int nextValue, nextPage, nextRecord;
    if (tempFiles[fileIndex] >> nextValue >> nextPage >> nextRecord)
    {
      currentValues.push_back(make_tuple(nextValue, nextPage, nextRecord, fileIndex));
      push_heap(currentValues.begin(), currentValues.end(), compareValues);
    }

    // If this is a new value
    if (value != currentValue)
    {
      // Process previous value if it exists
      if (currentValue != -1 && !currentValueRecords.empty())
      {
        // Check if we need a new index block
        if (distinctValuesInCurrentBlock >= MAX_DISTINCT_VALUES_PER_INDEX_BLOCK)
        {
          // Write counts and close current index file
          long pos = currentIndexFile.tellp();
          currentIndexFile.seekp(0);
          currentIndexFile << distinctValuesInCurrentBlock;
          currentIndexFile.seekp(pos);
          currentIndexFile.close();

          // Create new index file
          indexBlockCount++;
          distinctValuesInCurrentBlock = 0;

          currentIndexFileName = indexDir + tableName + "_" + columnName + "_Indexfile_" + to_string(indexBlockCount);
          currentIndexFile.open(currentIndexFileName);
          if (!currentIndexFile)
          {
            logger.log("SecondaryIndex::createIndex: Failed to create index file " + currentIndexFileName);
            return false;
          }

          // Reserve space for count
          currentIndexFile << "          " << endl;
        }

        // Create BP files for the current value
        vector<string> bpFileNames;
        int recordsProcessed = 0;
        int bpBlockCount = 0;

        while (recordsProcessed < currentValueRecords.size())
        {
          // Create block pointer file name
          string bpFileName = "BP_" + tableName + "_" + columnName + "_" + to_string(currentValue) + "_" + to_string(bpBlockCount);
          string bpFilePath = indexDir + bpFileName;
          ofstream bpFile(bpFilePath);

          if (!bpFile)
          {
            logger.log("SecondaryIndex::createIndex: Failed to create block pointer file " + bpFilePath);
            continue;
          }

          // Calculate how many records to write in this BP file
          int recordsToWrite = min(MAX_RECORDS_PER_BP_BLOCK, (int)currentValueRecords.size() - recordsProcessed);

          // Write record count
          bpFile << recordsToWrite << endl;

          // Write record pointers
          for (int i = 0; i < recordsToWrite; i++)
          {
            bpFile << currentValueRecords[recordsProcessed + i].first << " "
                   << currentValueRecords[recordsProcessed + i].second << endl;
          }

          bpFile.close();
          bpFileNames.push_back(bpFileName);
          bpBlockCount++;
          recordsProcessed += recordsToWrite;
        }

        // Write to index file: value and comma-separated list of BP files
        currentIndexFile << currentValue << " ";

        // Create comma-separated list of BP files
        for (size_t i = 0; i < bpFileNames.size(); i++)
        {
          currentIndexFile << bpFileNames[i];
          if (i < bpFileNames.size() - 1)
          {
            currentIndexFile << ",";
          }
        }
        currentIndexFile << endl;

        distinctValuesInCurrentBlock++;
        totalDistinctValues++;
      }

      // Start tracking new value
      currentValue = value;
      currentValueRecords.clear();
    }

    // Add record to current value's collection
    currentValueRecords.push_back({page, record});
  }

  // Process the last value if it exists
  if (currentValue != -1 && !currentValueRecords.empty())
  {
    // Check if we need a new index block
    if (distinctValuesInCurrentBlock >= MAX_DISTINCT_VALUES_PER_INDEX_BLOCK)
    {
      // Write counts and close current index file
      long pos = currentIndexFile.tellp();
      currentIndexFile.seekp(0);
      currentIndexFile << distinctValuesInCurrentBlock;
      currentIndexFile.seekp(pos);
      currentIndexFile.close();

      // Create new index file
      indexBlockCount++;
      distinctValuesInCurrentBlock = 0;

      currentIndexFileName = indexDir + tableName + "_" + columnName + "_Indexfile_" + to_string(indexBlockCount);
      currentIndexFile.open(currentIndexFileName);
      if (!currentIndexFile)
      {
        logger.log("SecondaryIndex::createIndex: Failed to create index file " + currentIndexFileName);
        return false;
      }

      // Reserve space for count
      currentIndexFile << "          " << endl;
    }

    // Create BP files for the current value
    vector<string> bpFileNames;
    int recordsProcessed = 0;
    int bpBlockCount = 0;

    while (recordsProcessed < currentValueRecords.size())
    {
      // Create block pointer file name
      string bpFileName = "BP_" + tableName + "_" + columnName + "_" + to_string(currentValue) + "_" + to_string(bpBlockCount);
      string bpFilePath = indexDir + bpFileName;
      ofstream bpFile(bpFilePath);

      if (!bpFile)
      {
        logger.log("SecondaryIndex::createIndex: Failed to create block pointer file " + bpFilePath);
        continue;
      }

      // Calculate how many records to write in this BP file
      int recordsToWrite = min(MAX_RECORDS_PER_BP_BLOCK, (int)currentValueRecords.size() - recordsProcessed);

      // Write record count
      bpFile << recordsToWrite << endl;

      // Write record pointers
      for (int i = 0; i < recordsToWrite; i++)
      {
        bpFile << currentValueRecords[recordsProcessed + i].first << " "
               << currentValueRecords[recordsProcessed + i].second << endl;
      }

      bpFile.close();
      bpFileNames.push_back(bpFileName);
      bpBlockCount++;
      recordsProcessed += recordsToWrite;
    }

    // Write to index file: value and comma-separated list of BP files
    currentIndexFile << currentValue << " ";

    // Create comma-separated list of BP files
    for (size_t i = 0; i < bpFileNames.size(); i++)
    {
      currentIndexFile << bpFileNames[i];
      if (i < bpFileNames.size() - 1)
      {
        currentIndexFile << ",";
      }
    }
    currentIndexFile << endl;

    distinctValuesInCurrentBlock++;
    totalDistinctValues++;
  }

  // Update count in the last index file
  long pos = currentIndexFile.tellp();
  currentIndexFile.seekp(0);
  currentIndexFile << distinctValuesInCurrentBlock;
  currentIndexFile.seekp(pos);
  currentIndexFile.close();

  // Clean up temporary files
  for (auto &file : tempFiles)
  {
    file.close();
  }

  for (const auto &fileName : tempFileNames)
  {
    filesystem::remove(fileName);
  }

  // Update in-memory index
  readIndex();

  logger.log("SecondaryIndex::createIndex: Index created successfully with " +
             to_string(indexBlockCount + 1) + " index blocks and " +
             to_string(totalDistinctValues) + " distinct values");
  return true;
}




bool SecondaryIndex::updateIndex(int newValue)
{
  logger.log("SecondaryIndex::updateIndex for value " + to_string(newValue));

  if (columnIndex == -1)
  {
    logger.log("SecondaryIndex::updateIndex: Invalid column index");
    return false;
  }

  // Ensure index is loaded
  if (indexEntries.empty())
  {
    readIndex();
  }

  // Check if the value already exists in the index
  int entryIndex = binarySearch(newValue);
  if (entryIndex != -1)
  {
    logger.log("SecondaryIndex::updateIndex: Value " + to_string(newValue) + " already exists in index");    
    return true; // Value already indexed
  }

  // Add the new value to the index
  IndexEntry newEntry;
  newEntry.fieldValue = newValue;
  newEntry.blockCount = 0;
  newEntry.recordCount = 0;

  // Create a new block pointer file for the new value
  string bpFileName = "BP_" + tableName + "_" + columnName + "_" + to_string(newValue) + "_0";
  string bpFilePath = "../data/indices/" + bpFileName;
  ofstream bpFile(bpFilePath);

  if (!bpFile)
  {
    logger.log("SecondaryIndex::updateIndex: Failed to create block pointer file " + bpFilePath);
    return false;
  }

  // Write an empty block pointer file
  bpFile << 0 << endl; // No records initially
  bpFile.close();

  // Update the new entry
  newEntry.blockPointers.push_back(bpFileName);
  newEntry.blockCount = 1;

  // Add the new entry to the in-memory index
  indexEntries.push_back(newEntry);

  // Sort the index entries to maintain order
  sort(indexEntries.begin(), indexEntries.end(), [](const IndexEntry &a, const IndexEntry &b)
       { return a.fieldValue < b.fieldValue; });

  // Write the updated index back to disk
  createIndex();

  logger.log("SecondaryIndex::updateIndex: Value " + to_string(newValue) + " added to index");
  return true;
}




string SecondaryIndex::getTableName() const
{
  return tableName;
}

string SecondaryIndex::getColumnName() const
{
  return columnName;
}

vector<pair<int, int>> SecondaryIndex::search(int value)
{
  logger.log("SecondaryIndex::search for value " + to_string(value));

  vector<pair<int, int>> results;

  // Ensure index is loaded
  if (indexEntries.empty())
  {
    readIndex();
  }

  // Find the index entry
  int entryIndex = binarySearch(value);
  if (entryIndex == -1)
  {
    logger.log("SecondaryIndex::search: Value " + to_string(value) + " not found in index");
    return results; // Not found
  }

  // Get list of block pointers for this value
  vector<string> blockPointers = indexEntries[entryIndex].blockPointers;

  // Read all block pointers for this value
  for (const string &bpName : blockPointers)
  {
    string bpFileName = "../data/indices/" + bpName;
    ifstream bpFile(bpFileName);

    if (!bpFile)
    {
      logger.log("SecondaryIndex::search: Failed to open block pointer file " + bpFileName);
      continue;
    }

    // Read number of records in this block
    int numRecords;
    bpFile >> numRecords;

    // Read all record pointers
    for (int i = 0; i < numRecords; i++)
    {
      int pageNo, recordNo;
      bpFile >> pageNo >> recordNo;
      results.push_back({pageNo, recordNo});
    }

    bpFile.close();
  }

  logger.log("SecondaryIndex::search: Found " + to_string(results.size()) + " records for value " + to_string(value));
  return results;
}

vector<pair<int, int>> SecondaryIndex::rangeSearch(int lowerBound, int upperBound)
{
  logger.log("SecondaryIndex::rangeSearch from " + to_string(lowerBound) + " to " + to_string(upperBound));

  vector<pair<int, int>> results;
  set<pair<int, int>> uniqueResults; // To eliminate duplicates

  // Ensure index is loaded
  if (indexEntries.empty())
  {
    readIndex();
  }

  // Iterate through index entries within the range
  for (const auto &entry : indexEntries)
  {
    if (entry.fieldValue >= lowerBound && entry.fieldValue <= upperBound)
    {
      // Get records for this value
      vector<pair<int, int>> valueResults = search(entry.fieldValue);

      // Add to unique results set
      for (const auto &result : valueResults)
      {
        uniqueResults.insert(result);
      }
    }
  }

  // Convert set back to vector
  results.insert(results.end(), uniqueResults.begin(), uniqueResults.end());

  logger.log("SecondaryIndex::rangeSearch: Found " + to_string(results.size()) + " total records in range");
  return results;
}

// Helper functions for SecondaryIndex class to support search operations
// Add these to the SecondaryIndex class in secondary_index.h

/*
 * Finds the index file that might contain the given value
 * Returns file path if found, empty string if not found
 */
string SecondaryIndex::findIndexFileForValue(int value)
{
  string indexDir = "../data/indices/";
  int fileIndex = 0;

  while (true)
  {
    string indexFileName = indexDir + tableName + "_" + columnName + "_Indexfile_" + to_string(fileIndex);
    ifstream indexFile(indexFileName);

    if (!indexFile)
    {
      // No more index files to check
      break;
    }

    // Read number of distinct values in this file
    int distinctCount;
    indexFile >> distinctCount;

    // Check if file is empty (should not happen in a properly created index)
    if (distinctCount <= 0)
    {
      indexFile.close();
      fileIndex++;
      continue;
    }

    // Read first and last value to determine range
    string line;
    getline(indexFile, line); // Skip the first line (already read distinctCount)

    // Get first value
    getline(indexFile, line);
    int firstValue = stoi(line.substr(0, line.find(' ')));

    // Skip to the last entry
    int skipCount = distinctCount - 2;
    while (skipCount > 0 && getline(indexFile, line))
    {
      skipCount--;
    }

    // Get last value if available
    if (getline(indexFile, line))
    {
      int lastValue = stoi(line.substr(0, line.find(' ')));

      // Check if value is in range
      if (value >= firstValue && value <= lastValue)
      {
        indexFile.close();
        return indexFileName;
      }
    }

    indexFile.close();
    fileIndex++;
  }

  return ""; // Value not found in any index file
}

/*
 * Performs binary search in an index file to find a specific value
 * Returns BP file names if found, empty vector if not found
 */
vector<string> SecondaryIndex::binarySearchInIndexFile(const string &indexFilePath, int targetValue)
{
  vector<string> bpFileNames;
  ifstream indexFile(indexFilePath);

  if (!indexFile)
  {
    return bpFileNames;
  }

  // Read number of distinct values in this file
  int distinctCount;
  indexFile >> distinctCount;

  // Read all entries into a vector for binary search
  // This is acceptable since a single index block has at most MAX_DISTINCT_VALUES_PER_INDEX_BLOCK (100) entries
  vector<pair<int, string>> entries;
  string line;
  getline(indexFile, line); // Skip the count line

  while (getline(indexFile, line))
  {
    size_t spacePos = line.find(' ');
    if (spacePos != string::npos)
    {
      int value = stoi(line.substr(0, spacePos));
      string bpList = line.substr(spacePos + 1);
      entries.push_back({value, bpList});
    }
  }

  indexFile.close();

  // Binary search
  int left = 0;
  int right = entries.size() - 1;

  while (left <= right)
  {
    int mid = left + (right - left) / 2;

    if (entries[mid].first == targetValue)
    {
      // Found the value, parse BP file names
      string bpList = entries[mid].second;
      size_t pos = 0;
      while ((pos = bpList.find(',')) != string::npos)
      {
        bpFileNames.push_back(bpList.substr(0, pos));
        bpList.erase(0, pos + 1);
      }
      bpFileNames.push_back(bpList); // Add the last BP file
      return bpFileNames;
    }

    if (entries[mid].first < targetValue)
    {
      left = mid + 1;
    }
    else
    {
      right = mid - 1;
    }
  }

  return bpFileNames; // Value not found
}

/*
 * Finds all values in the index that match the operator and search value
 * Returns vector of BP file names that contain matching records
 */
vector<string> SecondaryIndex::findMatchingBPFiles(const string &searchOperator, int searchValue)
{
  vector<string> matchingBPFiles;
  string indexDir = "../data/indices/";
  int fileIndex = 0;

  if (searchOperator == "==")
  {
    // For equality, just find the exact value
    string indexFilePath = findIndexFileForValue(searchValue);
    if (!indexFilePath.empty())
    {
      return binarySearchInIndexFile(indexFilePath, searchValue);
    }
  }
  else
  {
    // For inequality operators, we need to scan the index files
    while (true)
    {
      string indexFileName = indexDir + tableName + "_" + columnName + "_Indexfile_" + to_string(fileIndex);
      ifstream indexFile(indexFileName);

      if (!indexFile)
      {
        // No more index files to check
        break;
      }

      // Read number of distinct values
      int distinctCount;
      indexFile >> distinctCount;

      // Process each entry
      string line;
      getline(indexFile, line); // Skip the count line

      while (getline(indexFile, line))
      {
        size_t spacePos = line.find(' ');
        if (spacePos != string::npos)
        {
          int value = stoi(line.substr(0, spacePos));
          string bpList = line.substr(spacePos + 1);

          bool matches = false;

          if (searchOperator == "<")
          {
            matches = (value < searchValue);
          }
          else if (searchOperator == "<=")
          {
            matches = (value <= searchValue);
          }
          else if (searchOperator == ">")
          {
            matches = (value > searchValue);
          }
          else if (searchOperator == ">=")
          {
            matches = (value >= searchValue);
          }
          else if (searchOperator == "!=")
          {
            matches = (value != searchValue);
          }

          if (matches)
          {
            // Add BP files to result
            size_t pos = 0;
            string bpListCopy = bpList;
            while ((pos = bpListCopy.find(',')) != string::npos)
            {
              matchingBPFiles.push_back(bpListCopy.substr(0, pos));
              bpListCopy.erase(0, pos + 1);
            }
            matchingBPFiles.push_back(bpListCopy); // Add the last BP file
          }
        }
      }

      indexFile.close();
      fileIndex++;
    }
  }

  return matchingBPFiles;
}
