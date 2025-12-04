#include "matrix.h"
#include <fstream>
#include <sstream>
#include <iostream> // Include for debugging output


/**
 * @brief
 * SYNTAX: PRINT MATRIX matrix_name
 */

bool syntacticParsePRINTMATRIX()
{
  logger.log("syntacticParsePRINTMATRIX");
  if (tokenizedQuery.size() != 3)
  {
    cout << "SYNTAX ERROR" << endl;
    return false;
  }
  parsedQuery.queryType = PRINT_MATRIX;
  parsedQuery.printMatrixName = tokenizedQuery[2]; // Assuming the matrix name is the third token
  return true;
}

bool semanticParsePRINTMATRIX()
{
  logger.log("semanticParsePRINTMATRIX");
  string filePath = "../data/" + parsedQuery.printMatrixName; // No .csv here
  if (isFileExists(filePath))
  {
    return true;
  }
  cout << "SEMANTIC ERROR: Data file doesn't exist" << endl;
  return false;
}

void executePRINTMATRIX()
{
    logger.log("executePRINTMATRIX");
    Matrix *matrix = new Matrix(parsedQuery.printMatrixName);

    // Check if rotated blocks exist
    string blockPath = "../data/temp2/" + parsedQuery.printMatrixName + "_0_0.csv";
    ifstream blockFile(blockPath);
    
    if (blockFile.is_open()) {
        // Get block size by counting rows in first block
        string line;
        int rowCount = 0;
        while (getline(blockFile, line)) {
            rowCount++;
        }
        matrix->blockSize = rowCount;
        
        // Count number of blocks in row
        int blockCount = 0;
        while (true) {
            string testPath = "../data/temp2/" + parsedQuery.printMatrixName + "_0_" + to_string(blockCount) + ".csv";
            ifstream testFile(testPath);
            if (!testFile.is_open()) break;
            blockCount++;
        }
        matrix->noOfBlocksInRow = blockCount;
        
        // Calculate total matrix size
        matrix->matrixSize = matrix->blockSize * matrix->noOfBlocksInRow;
        
        matrix->print();
    }
    else {
        // No rotated blocks exist, load from original file
        string originalFilePath = "../data/" + parsedQuery.printMatrixName + ".csv";
        if (matrix->loadAndPartition(originalFilePath)) {
            matrix->print();
        }
        else {
            cout << "ERROR: Failed to load matrix from file." << endl;
        }
    }
    
    delete matrix;
}