#include "matrix.h"
#include <fstream>
#include <sstream>
#include <cmath>
#include <limits>

static const int BLOCK_SIZE_KB = 1; // Block size in KB
static const int BYTES_PER_BLOCK = BLOCK_SIZE_KB * 9;

Matrix::Matrix(const string &matrixName)
    : name(matrixName), matrixSize(0), blockSize(0), noOfBlocksInRow(0) {}

int Matrix::calculateBlockSize()
{
  int sizeof_int = sizeof(int);
  int NoOfInts = BYTES_PER_BLOCK / sizeof_int;
  return static_cast<int>(floor(sqrt(NoOfInts)));
}

void Matrix::calculateBlocksInRow()
{
  noOfBlocksInRow = static_cast<int>(ceil(static_cast<double>(matrixSize) / blockSize));
}

// Helper function to skip to the start of a specific row and column
void skipToPosition(ifstream &file, int row, int col)
{
  file.clear();
  file.seekg(0);

  // Skip to the correct row
  string unused;
  for (int i = 0; i < row; i++)
    getline(file, unused);

  // Skip to the correct column
  if (col > 0)
  {
    for (int i = 0; i < col; i++)
    {
      char ch;
      while (file.get(ch) && ch != ',')
      {
      }
    }
  }
}

// Read a single value from the current position
int readSingleValue(ifstream &file)
{
  string value;
  char ch;

  while (file.get(ch))
  {
    if (ch == ',' || ch == '\n')
      break;
    value += ch;
  }

  return value.empty() ? numeric_limits<int>::max() : stoi(value);
}

bool Matrix::loadAndPartition(const string &inputFilePath)
{
  ifstream file(inputFilePath);
  if (!file.is_open())
  {
    cerr << "Error opening file: " << inputFilePath << endl;
    return false;
  }

  // Calculate block size based on memory constraints
  blockSize = calculateBlockSize();

  // Count rows to determine matrix size
  matrixSize = 0;
  string line;
  while (getline(file, line))
    matrixSize++;

  // Calculate number of blocks needed
  calculateBlocksInRow();

  // Process one block at a time
  for (int blockRow = 0; blockRow < noOfBlocksInRow; blockRow++)
  {
    for (int blockCol = 0; blockCol < noOfBlocksInRow; blockCol++)
    {
      // Initialize empty block with padding
      vector<vector<int>> block(blockSize, vector<int>(blockSize, numeric_limits<int>::max()));

      // Calculate starting positions for this block
      int startRow = blockRow * blockSize;
      int startCol = blockCol * blockSize;

      // Read values for this block
      for (int localRow = 0; localRow < blockSize; localRow++)
      {
        int globalRow = startRow + localRow;
        if (globalRow >= matrixSize)
          break;

        // Position file pointer at the start of the needed values
        skipToPosition(file, globalRow, startCol);

        // Read values for this block row
        for (int localCol = 0; localCol < blockSize; localCol++)
        {
          int globalCol = startCol + localCol;
          if (globalCol >= matrixSize)
            break;

          block[localRow][localCol] = readSingleValue(file);
        }
      }

      // Save the block
      string filename = name + "_" + to_string(blockRow) + "_" + to_string(blockCol) + ".csv";
      cout << "Saving block to file: " << filename << endl;
      saveBlock(block, filename);
    }
  }

  file.close();
  return true;
}

void Matrix::saveBlock(const vector<vector<int>> &block, const string &filename)
{
  ofstream file("../data/temp2/" + filename);
  for (const auto &row : block)
  {
    for (size_t j = 0; j < row.size(); j++)
    {
      if (row[j] == numeric_limits<int>::max())
        file << "null";
      else
        file << row[j];
      file << (j == row.size() - 1 ? "" : ",");
    }
    file << "\n";
  }
  file.close();
}

vector<vector<int>> Matrix::getBlock(int blockRow, int blockCol) const
{
    string filename = name + "_" + to_string(blockRow) + "_" + to_string(blockCol) + ".csv";
    vector<vector<int>> block;

    try {
        ifstream file("../data/temp2/" + filename);
        if (!file.is_open()) {
            return block;
        }

        string line;
        while (getline(file, line)) {
            vector<int> row;
            stringstream ss(line);
            string cell;
            
            while (getline(ss, cell, ',')) {
                if (cell == "null" || cell.empty()) {
                    row.push_back(numeric_limits<int>::max());
                } else {
                    try {
                        row.push_back(stoi(cell));
                    } catch (...) {
                        row.push_back(numeric_limits<int>::max());
                    }
                }
            }
            block.push_back(row);
        }
        file.close();
    }
    catch (const exception& e) {
        cerr << "Error reading block file " << filename << ": " << e.what() << endl;
    }

    return block;
}
bool Matrix::print()
{
    if (matrixSize == 0 || blockSize == 0 || noOfBlocksInRow == 0)
    {
        cerr << "Matrix not properly initialized" << endl;
        return false;
    }

    try {
        int maxColumns = min(matrixSize, 20); // Limit to 20 columns
        int maxRows = min(matrixSize, 20);    // Limit to 20 rows

        for (int currentRow = 0; currentRow < maxRows; currentRow++)
        {
            int blockRow = currentRow / blockSize;
            int localRow = currentRow % blockSize;

            int printedColumns = 0;

            for (int blockCol = 0; blockCol < noOfBlocksInRow && printedColumns < maxColumns; blockCol += 2)
            {
                // Load only two blocks at a time
                vector<vector<int>> block1 = getBlock(blockRow, blockCol);
                vector<vector<int>> block2;
                if (blockCol + 1 < noOfBlocksInRow)
                {
                    block2 = getBlock(blockRow, blockCol + 1);
                }

                // Print block1
                if (!block1.empty() && localRow < block1.size())
                {
                    for (int localCol = 0; localCol < block1[localRow].size(); localCol++)
                    {
                        int globalCol = blockCol * blockSize + localCol;
                        if (globalCol >= maxColumns) break; // Stop at 20 columns

                        int value = block1[localRow][localCol];
                        if (value != numeric_limits<int>::max())
                        {
                            cout << value<<" ";
                            printedColumns++;
                            if (printedColumns < maxColumns) cout << "\t";
                        }

                        if (printedColumns >= maxColumns) break;
                    }
                }

                // Print block2
                if (!block2.empty() && localRow < block2.size())
                {
                    for (int localCol = 0; localCol < block2[localRow].size(); localCol++)
                    {
                        int globalCol = (blockCol + 1) * blockSize + localCol;
                        if (globalCol >= maxColumns) break; // Stop at 20 columns

                        int value = block2[localRow][localCol];
                        if (value != numeric_limits<int>::max())
                        {
                            cout << value;
                            printedColumns++;
                            if (printedColumns < maxColumns) cout << "\t";
                        }

                        if (printedColumns >= maxColumns) break;
                    }
                }

                if (printedColumns >= maxColumns) break;
            }
            cout << endl; // Print newline after each row
        }
        return true;
    }
    catch (const exception& e) {
        cerr << "Error in print function: " << e.what() << endl;
        return false;
    }
}


static void transposeBlock(vector<vector<int>> &block) {
    if (block.empty())
        return;
    int rows = block.size();
    int cols = block[0].size();
    // In case the block is not square (e.g. at the matrix boundaries), build a new block.
    vector<vector<int>> transposed(cols, vector<int>(rows, numeric_limits<int>::max()));
    for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
            transposed[j][i] = block[i][j];
        }
    }
    block = std::move(transposed);
}

// Helper: Reverse each row of the block.
static void reverseBlockRows(vector<vector<int>> &block) {
    for (auto &row : block) {
        std::reverse(row.begin(), row.end());
    }
}

bool Matrix::rotate() {
    // Check that the matrix has been partitioned.
    if (matrixSize == 0 || blockSize == 0 || noOfBlocksInRow == 0) {
        cerr << "Matrix not properly initialized" << endl;
        return false;
    }

    int n = noOfBlocksInRow;  // number of blocks in each row (and column)

    // **** STEP 1: Transpose the matrix ****

    // (a) Process diagonal blocks (only one block in memory at a time)
    for (int i = 0; i < n; i++) {
        string filename = name + "_" + to_string(i) + "_" + to_string(i) + ".csv";
        vector<vector<int>> block = getBlock(i, i);
        if (!block.empty()) {
            transposeBlock(block);
            saveBlock(block, filename);
        }
    }

    // (b) Process off-diagonal blocks in pairs (load exactly 2 blocks at a time)
    for (int i = 0; i < n; i++) {
        for (int j = i + 1; j < n; j++) {
            // Get the two symmetric blocks.
            vector<vector<int>> block1 = getBlock(i, j);
            vector<vector<int>> block2 = getBlock(j, i);
            // Transpose each block in memory.
            if (!block1.empty()) transposeBlock(block1);
            if (!block2.empty()) transposeBlock(block2);
            // Swap the blocks by saving them in each other's positions.
            saveBlock(block1, name + "_" + to_string(j) + "_" + to_string(i) + ".csv");
            saveBlock(block2, name + "_" + to_string(i) + "_" + to_string(j) + ".csv");
        }
    }

    // **** STEP 2: Reverse each row (horizontal flip) ****
    for (int blockRow = 0; blockRow < n; blockRow++) {
        int left = 0;
        int right = n - 1;
        while (left < right) {
            // Load the two blocks from the current row that need to be swapped.
            vector<vector<int>> leftBlock = getBlock(blockRow, left);
            vector<vector<int>> rightBlock = getBlock(blockRow, right);

            // Reverse each row within each block.
            reverseBlockRows(leftBlock);
            reverseBlockRows(rightBlock);

            // Swap the blocks by writing them to each other's file locations.
            saveBlock(leftBlock, name + "_" + to_string(blockRow) + "_" + to_string(right) + ".csv");
            saveBlock(rightBlock, name + "_" + to_string(blockRow) + "_" + to_string(left) + ".csv");

            left++;
            right--;
        }
        // If there is a middle block (when n is odd), process it separately.
        if (left == right) {
            vector<vector<int>> middleBlock = getBlock(blockRow, left);
            reverseBlockRows(middleBlock);
            saveBlock(middleBlock, name + "_" + to_string(blockRow) + "_" + to_string(left) + ".csv");
        }
    }

    return true;
}

bool Matrix::exportMatrix()
{
    // Create output file using matrix name
    string outputPath = "../data/"+ name + ".csv";
    ofstream outputFile(outputPath);
    if (!outputFile.is_open())
    {
        cerr << "Error creating output file: " << outputPath << endl;
        return false;
    }

    // Process one row of blocks at a time
    for (int blockRow = 0; blockRow < noOfBlocksInRow; blockRow++)
    {
        // Read all blocks in this row
        vector<vector<vector<int>>> rowBlocks(noOfBlocksInRow);
        
        for (int blockCol = 0; blockCol < noOfBlocksInRow; blockCol++)
        {
            // Construct block filename
            string blockPath = "../data/temp2/"+name + "_" + to_string(blockRow) + "_" + to_string(blockCol) + ".csv";
            // Read block from file
            vector<vector<int>> block;
            ifstream blockFile(blockPath);
            if (!blockFile.is_open())
            {
                cerr << "Error opening block file: " << blockPath << endl;
                return false;
            }
            cout<<"Reading block from file: "<<blockPath<<endl;
            // Read block data
            string line;
            while (getline(blockFile, line))
            {
                vector<int> row;
                stringstream ss(line);
                string value;
                while (getline(ss, value, ','))
                {
                    // Skip padding values
                    try {
                        int intValue = stoi(value);
                        if (intValue != numeric_limits<int>::max())
                        {
                            row.push_back(intValue);
                        }
                    } catch (const std::invalid_argument& e) {
                        cerr << "";
                    }
                }
                if (!row.empty())  // Only add non-empty rows
                {
                    block.push_back(row);
                }
            }
            blockFile.close();
            rowBlocks[blockCol] = block;
        }

        // Write this row of blocks to the output file
        for (int localRow = 0; localRow < blockSize; localRow++)
        {
            // Check if we've exceeded matrix size
            int globalRow = blockRow * blockSize + localRow;
            if (globalRow >= matrixSize)
                break;

            // Combine rows from all blocks in this row
            for (int blockCol = 0; blockCol < noOfBlocksInRow; blockCol++)
            {
                const auto& block = rowBlocks[blockCol];
                // Skip if we don't have this row in the block
                if (localRow >= block.size())
                    continue;
                
                const auto& blockRow = block[localRow];
                for (int col = 0; col < blockRow.size(); col++)
                {
                    outputFile << blockRow[col];
                    // Add comma if not last value in row
                    if (blockCol < noOfBlocksInRow - 1 || col < blockRow.size() - 1)
                    {
                        outputFile << ",";
                    }
                }
            }
            outputFile << endl;
        }
    }
    outputFile.close();
    return true;
}


bool Matrix::checkAntiSymmetric(const Matrix &other) {
    // Verify matrices have compatible dimensions and block configurations
    if (matrixSize != other.matrixSize ||
        blockSize != other.blockSize ||
        noOfBlocksInRow != other.noOfBlocksInRow) {
        cerr << "Matrix dimensions or block configurations do not match." << endl;
        return false;
    }

    // Process blocks one pair at a time to maintain memory constraint
    for (int i = 0; i < noOfBlocksInRow; i++) {
        for (int j = 0; j < noOfBlocksInRow; j++) {
            // Load block from matrix A
            vector<vector<int>> blockA = getBlock(i, j);
            if (blockA.empty()) continue;

            // Load corresponding block from matrix B (transposed position)
            vector<vector<int>> blockB = other.getBlock(j, i);
            if (blockB.empty()) continue;

            // Compare elements
            for (int r = 0; r < blockSize; r++) {
                for (int c = 0; c < blockSize; c++) {
                    // Calculate global positions to check matrix boundaries
                    int globalRow = i * blockSize + r;
                    int globalCol = j * blockSize + c;
                    
                    // Only check elements within matrix boundaries
                    if (globalRow >= matrixSize || globalCol >= matrixSize) 
                        continue;

                    // For anti-symmetry: A[i][j] = -B[j][i]
                    // Check if corresponding elements in B have opposite sign
                    if (blockA[r][c] != numeric_limits<int>::max() && 
                        blockB[c][r] != numeric_limits<int>::max()) {
                        
                        if (blockA[r][c] != -blockB[c][r]) {
                            cout << "Anti-symmetry violation at position (" 
                                 << globalRow << "," << globalCol << "): "
                                 << "A[" << globalRow << "][" << globalCol << "] = " 
                                 << blockA[r][c] << ", "
                                 << "B[" << globalCol << "][" << globalRow << "] = " 
                                 << blockB[c][r] << endl;
                            return false;
                        }
                    }
                    // If one element is null (max int) but other isn't, matrices aren't anti-symmetric
                    else if ((blockA[r][c] == numeric_limits<int>::max() && 
                             blockB[c][r] != numeric_limits<int>::max()) ||
                            (blockA[r][c] != numeric_limits<int>::max() && 
                             blockB[c][r] == numeric_limits<int>::max())) {
                        cout << "Null value mismatch at position (" 
                             << globalRow << "," << globalCol << ")" << endl;
                        return false;
                    }
                }
            }
        }
    }
    return true;
}

bool Matrix::crossTranspose(const Matrix &other) {
    // Verify matrices have compatible dimensions and block configurations
    if (matrixSize != other.matrixSize ||
        blockSize != other.blockSize ||
        noOfBlocksInRow != other.noOfBlocksInRow) {
        cerr << "Matrix dimensions or block configurations do not match." << endl;
        return false;
    }

    // Process blocks one pair at a time
    for (int i = 0; i < noOfBlocksInRow; i++) {
        for (int j = 0; j < noOfBlocksInRow; j++) {
            // Load blocks from both matrices
            vector<vector<int>> blockA = getBlock(i, j);
            vector<vector<int>> blockB = other.getBlock(j, i);

            if (blockA.empty() && blockB.empty()) continue;

            // Transpose and swap in place
            for (int r = 0; r < blockSize; r++) {
                for (int c = 0; c < blockSize; c++) {  // Change 'c = r' to 'c = 0'
                    // Calculate global positions
                    int globalRowA = i * blockSize + r;
                    int globalColA = j * blockSize + c;
                    int globalRowB = j * blockSize + c;
                    int globalColB = i * blockSize + r;
            
                    if (globalRowA >= matrixSize || globalColA >= matrixSize ||
                        globalRowB >= matrixSize || globalColB >= matrixSize) 
                        continue;
            
                    swap(blockA[r][c], blockB[c][r]);  // Ensures full swap
                }
            }            
            

            // Save blocks back to files
            saveBlock(blockA, name + "_" + to_string(i) + "_" + to_string(j) + ".csv");
            saveBlock(blockB, other.name + "_" + to_string(j) + "_" + to_string(i) + ".csv");
        }
    }
    return true;
}
