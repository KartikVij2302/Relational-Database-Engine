#include "matrix.h"

/**
 * @brief
 * SYNTAX: ROTATE_MATRIX matrix_name
 */

bool syntacticParseROTATEMATRIX()
{
  logger.log("syntacticParseROTATEMATRIX");
  if (tokenizedQuery.size() != 2)
  {
    cout << "SYNTAX ERROR" << endl;
    return false;
  }
  parsedQuery.queryType = ROTATE_MATRIX;
  parsedQuery.rotateMatrixName = tokenizedQuery[1]; // Assuming the matrix name is the third token
  return true;
}

bool semanticParseROTATEMATRIX()
{
  logger.log("semanticParseROTATEMATRIX");
  string filePath = "../data/" + parsedQuery.rotateMatrixName; // No  .csv here
  if (isFileExists(filePath))
  {
    return true;
  }
  cout << "SEMANTIC ERROR: Data file doesn't exist" << endl;
  return false;
}


void executeROTATEMATRIX()
{
    logger.log("executeROTATEMATRIX");
    Matrix *matrix = new Matrix(parsedQuery.rotateMatrixName);

    // ✅ Check if rotated blocks exist
    string blockPath = "../data/temp2/" + parsedQuery.rotateMatrixName + "_0_0.csv";
    ifstream blockFile(blockPath);
    if (blockFile.is_open()) {
        // ✅ Determine block size (row count in first block)
        string line;
        int rowCount = 0;
        while (getline(blockFile, line)) {
            rowCount++;
        }
        blockFile.close();
        matrix->blockSize = rowCount;

        // ✅ Count number of blocks in a row
        int blockCount = 0;
        while (true) {
            string testPath = "../data/temp2/" + parsedQuery.rotateMatrixName + "_0_" + to_string(blockCount) + ".csv";
            ifstream testFile(testPath);
            if (!testFile.is_open()) break;
            testFile.close();
            blockCount++;
        }
        matrix->noOfBlocksInRow = blockCount;

        // ✅ Calculate total matrix size
        matrix->matrixSize = matrix->blockSize * matrix->noOfBlocksInRow;

        // ✅ Rotate existing matrix
        matrix->rotate();
    }
    else {
        // ✅ No rotated blocks exist, load from original file
        string originalFilePath = "../data/" + parsedQuery.rotateMatrixName + ".csv";
        if (matrix->loadAndPartition(originalFilePath)) {
            matrix->rotate();

            // ✅ Save rotated matrix back to temp2/
            // matrix->save("../data/temp2/");  // Uncomment this if saving is needed
        }
        else {
            cout << "ERROR: Failed to load matrix from file." << endl;
        }
    }

    delete matrix;
}



