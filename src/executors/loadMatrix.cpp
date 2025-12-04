// #include "global.h"
#include "matrix.h"
#include "matrixCatalogue.h" // Include the MatrixCatalogue header

/**
 * @brief
 * SYNTAX: LOAD MATRIX matrix_name
 */
bool syntacticParseLOADMATRIX()
{
  logger.log("syntacticParseLOADMATRIX");
  if (tokenizedQuery.size() != 3)
  {
    cout << "SYNTAX ERROR" << endl;
    return false;
  }
  parsedQuery.queryType = LOAD_MATRIX;
  parsedQuery.loadMatrixName = tokenizedQuery[2]; // Assuming the matrix name is the third token
  return true;
}

bool semanticParseLOADMATRIX()
{
  logger.log("semanticParseLOADMATRIX");
  string filePath = "../data/" + parsedQuery.loadMatrixName; // No .csv here
  if (isFileExists(filePath))
  {
    return true;
  }
  cout << "SEMANTIC ERROR: Data file doesn't exist" << endl;
  return false;
}

void executeLOADMATRIX()
{
  logger.log("executeLOADMATRIX");
  string filePath = "../data/" + parsedQuery.loadMatrixName + ".csv";
  Matrix *matrix = new Matrix(parsedQuery.loadMatrixName);
  matrixCatalogue.insertMatrix(matrix); // Insert the loaded matrix into the catalogue
  if (matrix->loadAndPartition(filePath))
  {
    cout << "Loaded Matrix. Matrix Size: " << matrix->matrixSize
         << " Blocks in a row Count: " << matrix->noOfBlocksInRow
         << " No of rows in a block: " << matrix->blockSize << endl;
  }
  else
  {
    cout << "ERROR: Failed to load matrix from file." << endl;
  }
}
