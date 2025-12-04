#include "externalsort.h"
#include <ctime>
#include <iomanip>
#include <sstream>
#include <algorithm>

ExternalSort::ExternalSort(Table *table,
                           const vector<string> &sortColumns,
                           const vector<bool> &sortDirections)
    : table(table), sortColumns(sortColumns), sortDirections(sortDirections) {}

string ExternalSort::generateTempFileName(int runNumber)
{
  // Generate a unique temporary filename
  auto now = std::chrono::system_clock::now();
  auto now_c = std::chrono::system_clock::to_time_t(now);
  std::stringstream ss;
  ss << "../data/temp/" << table->tableName
     << "_run_" << runNumber
     << "_" << std::put_time(std::localtime(&now_c), "%Y%m%d%H%M%S")
     << ".tmp";
  return ss.str();
}

bool ExternalSort::compareRows(const vector<int> &a, const vector<int> &b)
{
  for (size_t i = 0; i < sortColumns.size(); ++i)
  {
    int colIndex = table->getColumnIndex(sortColumns[i]);
    if (a[colIndex] != b[colIndex])
    {
      return sortDirections[i] ? a[colIndex] < b[colIndex] : a[colIndex] > b[colIndex];
    }
  }
  return false;
}

void ExternalSort::sortRun(vector<vector<int>> &run)
{
  // Sort the run using the custom comparator
  std::sort(run.begin(), run.end(),
            [this](const vector<int> &a, const vector<int> &b)
            {
              return this->compareRows(a, b);
            });
}

vector<vector<int>> ExternalSort::readBlockFromFile(ifstream &file)
{
  vector<vector<int>> block;
  vector<int> row(table->columnCount, 0);

  // Read block of rows
  for (int i = 0; i < table->maxRowsPerBlock && !file.eof(); ++i)
  {
    bool validRow = true;
    for (int j = 0; j < table->columnCount; ++j)
    {
      if (!(file >> row[j]))
      {
        validRow = false;
        break;
      }
    }
    if (validRow)
    {
      block.push_back(row);
    }
  }

  return block;
}

void ExternalSort::writeBlockToFile(ofstream &file, const vector<vector<int>> &block)
{
  for (const auto &row : block)
  {
    for (size_t j = 0; j < row.size(); ++j)
    {
      file << row[j];
      if (j < row.size() - 1)
        file << " ";
    }
    file << "\n";
  }
}

void ExternalSort::performExternalSort()
{
  // cout<<("ExternalSort::performExternalSort")<<endl;

  // Vector to store temporary run files
  vector<string> runFiles;

  // Cursor to read original table
  Cursor cursor = table->getCursor();

  // Sorting Phase: Create sorted runs
  int runNumber = 0;
  vector<vector<int>> currentRun;

  // Read and process rows
  vector<int> row;
  while (!(row = cursor.getNext()).empty())
  {
    currentRun.push_back(row);

    // If run is full, sort and write to temporary file
    if (currentRun.size() >= table->maxRowsPerBlock * EXTERNAL_SORT_BUFFER_BLOCKS)
    {
      // Sort the run
      sortRun(currentRun);

      // Write to temporary file
      string tempFileName = generateTempFileName(runNumber++);
      ofstream tempFile(tempFileName);

      for (const auto &sortedRow : currentRun)
      {
        for (size_t j = 0; j < sortedRow.size(); ++j)
        {
          tempFile << sortedRow[j];
          if (j < sortedRow.size() - 1)
            tempFile << " ";
        }
        tempFile << "\n";
      }
      tempFile.close();

      // Store the temporary file name
      runFiles.push_back(tempFileName);

      // Reset current run
      currentRun.clear();
    }
  }

  // Handle last run if not empty
  if (!currentRun.empty())
  {
    // Sort the last run
    sortRun(currentRun);

    // Write to temporary file
    string tempFileName = generateTempFileName(runNumber++);
    ofstream tempFile(tempFileName);

    for (const auto &sortedRow : currentRun)
    {
      for (size_t j = 0; j < sortedRow.size(); ++j)
      {
        tempFile << sortedRow[j];
        if (j < sortedRow.size() - 1)
          tempFile << " ";
      }
      tempFile << "\n";
    }
    tempFile.close();

    // Store the temporary file name
    runFiles.push_back(tempFileName);
  }

  // Merging Phase
  while (runFiles.size() > 1)
  {
    vector<string> newRunFiles;

    // Merge runs in groups
    for (size_t i = 0; i < runFiles.size();)
    {
      // Determine how many runs to merge in this pass
      size_t runsToMerge = std::min(
          static_cast<size_t>(EXTERNAL_SORT_BUFFER_BLOCKS - 1),
          runFiles.size() - i);

      // Open input files
      vector<ifstream> inputFiles;
      for (size_t j = 0; j < runsToMerge; ++j)
      {
        inputFiles.emplace_back(runFiles[i + j]);
      }

      // Create output file for merged run
      string mergedRunFile = generateTempFileName(runNumber++);
      ofstream mergedFile(mergedRunFile);

      // Custom comparator for priority queue
      auto comp = [this](const pair<vector<int>, size_t> &a,
                         const pair<vector<int>, size_t> &b)
      {
        return !this->compareRows(a.first, b.first);
      };

      // Explicitly specify the priority queue type
      priority_queue<
          pair<vector<int>, size_t>,
          vector<pair<vector<int>, size_t>>,
          decltype(comp)>
          pq(comp);

      // Read first block from each file
      vector<vector<vector<int>>> currentBlocks(runsToMerge);
      for (size_t j = 0; j < runsToMerge; ++j)
      {
        currentBlocks[j] = readBlockFromFile(inputFiles[j]);
        if (!currentBlocks[j].empty())
        {
          pq.push(make_pair(currentBlocks[j][0], j));
        }
      }

      // Merge process
      vector<vector<int>> outputBlock;
      while (!pq.empty())
      {
        auto topPair = pq.top();
        pq.pop();

        auto row = topPair.first;
        auto fileIndex = topPair.second;

        // Write row to output
        outputBlock.push_back(row);

        // If output block is full, write to file
        if (outputBlock.size() >= table->maxRowsPerBlock)
        {
          writeBlockToFile(mergedFile, outputBlock);
          outputBlock.clear();
        }

        // Remove this row from its block
        currentBlocks[fileIndex].erase(currentBlocks[fileIndex].begin());

        // If block is empty, read next block
        if (currentBlocks[fileIndex].empty())
        {
          currentBlocks[fileIndex] = readBlockFromFile(inputFiles[fileIndex]);
        }

        // If new block is not empty, add to priority queue
        if (!currentBlocks[fileIndex].empty())
        {
          pq.push(make_pair(currentBlocks[fileIndex][0], fileIndex));
        }
      }

      // Write any remaining rows
      if (!outputBlock.empty())
      {
        writeBlockToFile(mergedFile, outputBlock);
      }

      mergedFile.close();
      newRunFiles.push_back(mergedRunFile);

      // Close input files
      for (auto &file : inputFiles)
      {
        file.close();
      }

      // Move to next group of runs
      i += runsToMerge;
    }

    // Replace old runs with new merged runs
    runFiles = newRunFiles;
  }

  // Final step: Replace original table with sorted table

  if (!runFiles.empty())
  {
    // DEBUG: Add logging to track the final sorted file
    // cout<<("Final sorted file: " + runFiles[0])<<endl;
    ifstream finalSortedFile(runFiles[0]);

    // DEBUG: Check if file is open
    if (!finalSortedFile.is_open())
    {
      cout<<("ERROR: Unable to open final sorted file")<<endl;
      return;
    }

    // Reset table blocks and statistics
    table->blockCount = 0;
    table->rowsPerBlockCount.clear();
    table->distinctValuesInColumns.assign(table->columnCount, unordered_set<int>());
    table->distinctValuesPerColumnCount.assign(table->columnCount, 0);
    table->rowCount = 0;

    // DEBUG: Add file content verification
    vector<vector<int>> debugFileContents;

    // Prepare to write sorted rows back to pages
    vector<int> pageRow(table->columnCount, 0);
    vector<vector<int>> rowsInPage(table->maxRowsPerBlock, pageRow);
    int pageCounter = 0;

    // Read and write sorted rows
    vector<int> row(table->columnCount);
    int rowReadCount = 0;
    while (finalSortedFile >> row[0])
    {
      for (int i = 1; i < table->columnCount; ++i)
      {
        finalSortedFile >> row[i];
      }

      // DEBUG: Log row reading
      rowReadCount++;
      debugFileContents.push_back(row);

      // Copy row to page
      for (int columnCounter = 0; columnCounter < table->columnCount; columnCounter++)
      {
        rowsInPage[pageCounter][columnCounter] = row[columnCounter];
      }

      pageCounter++;
      table->updateStatistics(row);

      // When page is full, write page and reset
      if (pageCounter == table->maxRowsPerBlock)
      {
        bufferManager.writePage(table->tableName, table->blockCount, rowsInPage, pageCounter);
        table->blockCount++;
        table->rowsPerBlockCount.emplace_back(pageCounter);
        pageCounter = 0;
      }
    }

    // Write any remaining rows in the last page
    if (pageCounter > 0)
    {
      rowsInPage.resize(pageCounter);
      bufferManager.writePage(table->tableName, table->blockCount, rowsInPage, pageCounter);
      table->blockCount++;
      table->rowsPerBlockCount.emplace_back(pageCounter);
    }

    // DEBUG: Log read rows and file contents
    // cout<<("Total rows read from sorted file: " + to_string(rowReadCount))<<endl;
    // cout<<("Rows in sorted file contents:")<<endl;
    // for (const auto &debugRow : debugFileContents)
    // {
    //   std::stringstream ss;
    //   for (int val : debugRow)
    //   {
    //     ss << val << " ";
    //   }
    //   cout<<(ss.str())<<endl;
    // }

    // Clean up temporary files
    cleanupTempFiles(runFiles);
    bufferManager.clearPool();
  }

  // Print table to confirm sorting
  table->print();
}

void ExternalSort::cleanupTempFiles(const vector<string> &tempFiles)
{
  for (const auto &file : tempFiles)
  {
    remove(file.c_str());
  }
}














// Hi
void ExternalSort::performOrderBy(const string &resultTableName,
                                  const string &sortColumn,
                                  bool isAscending)
{
  // Create a new table with the same structure as the original table
  Table *resultTable = new Table(resultTableName, table->getColumnNames());

  // Copy column names from the original table
  for (int i = 0; i < table->columnCount; ++i)
  {
    resultTable->columns[i] = table->columns[i];
  }

  // cout << "[DEBUG] Created result table: " << resultTableName << endl;
  // cout << "[DEBUG] Starting performOrderBy on column: " << sortColumn
      //  << " with isAscending: " << isAscending << endl;

  // Prepare sorting parameters
  vector<string> sortColumns = {sortColumn};
  vector<bool> sortDirections = {isAscending};

  // Set the new table's parameters to match the original
  resultTable->maxRowsPerBlock = table->maxRowsPerBlock;

  // Temporary vector to store run files
  vector<string> runFiles;

  // Cursor to read original table
  Cursor cursor = table->getCursor();

  // Sorting Phase: Create sorted runs
  int runNumber = 0;
  vector<vector<int>> currentRun;

  // Read and process rows
  vector<int> row;
  int colIndex = table->getColumnIndex(sortColumn);

  while (!(row = cursor.getNext()).empty())
  {
    currentRun.push_back(row);

    // If run is full, sort and write to temporary file
    if (currentRun.size() >= table->maxRowsPerBlock * EXTERNAL_SORT_BUFFER_BLOCKS)
    {
      // Sort the run using the specified column
      std::sort(currentRun.begin(), currentRun.end(),
                [this, colIndex, isAscending](const vector<int> &a, const vector<int> &b)
                {
                  return isAscending ? a[colIndex] < b[colIndex] : a[colIndex] > b[colIndex];
                });

      // Write to temporary file
      string tempFileName = generateTempFileName(runNumber++);
      ofstream tempFile(tempFileName);

      writeBlockToFile(tempFile, currentRun);
      tempFile.close();

      // Store the temporary file name
      runFiles.push_back(tempFileName);

      // Reset current run
      currentRun.clear();
    }
  }

  // Handle last run if not empty
  if (!currentRun.empty())
  {
    // cout << "[DEBUG] Sorting the last run of size " << currentRun.size() << endl;

    // Sort the last run
    std::sort(currentRun.begin(), currentRun.end(),
              [this, colIndex, isAscending](const vector<int> &a, const vector<int> &b)
              {
                return isAscending ? a[colIndex] < b[colIndex] : a[colIndex] > b[colIndex];
              });

    // Write to temporary file
    string tempFileName = generateTempFileName(runNumber++);
    ofstream tempFile(tempFileName);

    writeBlockToFile(tempFile, currentRun);
    tempFile.close();

    // Store the temporary file name
    runFiles.push_back(tempFileName);
  }

  // Final step: Write sorted data to the new table
  if (!runFiles.empty())
  {
    ifstream finalSortedFile(runFiles[0]);

    // Reset result table blocks and statistics
    resultTable->blockCount = 0;
    resultTable->rowsPerBlockCount.clear();
    resultTable->distinctValuesInColumns.assign(resultTable->columnCount, unordered_set<int>());
    resultTable->distinctValuesPerColumnCount.assign(resultTable->columnCount, 0);
    resultTable->rowCount = 0;

    // Prepare to write sorted rows back to pages
    vector<vector<int>> currentPage;
    int pageCounter = 0;

    // Read and write sorted rows
    vector<int> row(resultTable->columnCount, 0);

    // Read all rows from the sorted file
    while (true)
    {
      // Read a complete row
      bool rowReadSuccessful = true;
      for (int i = 0; i < resultTable->columnCount; ++i)
      {
        if (!(finalSortedFile >> row[i]))
        {
          rowReadSuccessful = false;
          break;
        }
      }

      // If row read failed and we've reached end of file, break
      if (!rowReadSuccessful)
      {
        // Check if we've actually reached end of file
        if (finalSortedFile.eof())
          break;

        // cout << "[ERROR] Error reading row from sorted file" << endl;
        break;
      }

      // Add row to current page
      currentPage.push_back(row);
      resultTable->updateStatistics(row);
      pageCounter++;

      // When page is full, write page and reset
      if (pageCounter == resultTable->maxRowsPerBlock)
      {
        // cout << "[DEBUG] Writing page " << resultTable->blockCount
        //      << " with " << pageCounter << " rows." << endl;

        bufferManager.writePage(resultTable->tableName, resultTable->blockCount, currentPage, pageCounter);
        resultTable->blockCount++;
        resultTable->rowsPerBlockCount.emplace_back(pageCounter);

        // Reset for next page
        currentPage.clear();
        pageCounter = 0;
      }
    }

    // Write any remaining rows in the last page
    if (!currentPage.empty())
    {
      // cout << "[DEBUG] Writing final partial page with " << currentPage.size() << " rows." << endl;

      bufferManager.writePage(resultTable->tableName, resultTable->blockCount, currentPage, currentPage.size());
      resultTable->blockCount++;
      resultTable->rowsPerBlockCount.emplace_back(currentPage.size());
    }

    // cout << "[DEBUG] Finished writing pages. Total blocks: " << resultTable->blockCount << endl;

    // Clean up
    cleanupTempFiles(runFiles);
    bufferManager.clearPool();

    // Add the new table to the table catalogue
    tableCatalogue.insertTable(resultTable);
  }
  else
  {
    cout << "[ERROR] No sorted files generated" << endl;
  }
}