// matrixCatalogue.cpp
#include "matrixCatalogue.h"

MatrixCatalogue matrixCatalogue;
void MatrixCatalogue::insertMatrix(Matrix *matrix)
{
  logger.log("MatrixCatalogue::insertMatrix");
  this->matrices[matrix->name] = matrix;
}

void MatrixCatalogue::deleteMatrix(string matrixName)
{
  logger.log("MatrixCatalogue::deleteMatrix");

  // First, delete all temporary block files
  if (this->matrices[matrixName] != nullptr)
  {
    int noOfBlocks = this->matrices[matrixName]->noOfBlocksInRow;
    for (int i = 0; i < noOfBlocks; i++)
    {
      for (int j = 0; j < noOfBlocks; j++)
      {
        string filename = "../data/temp/" + matrixName + "_" + to_string(i) + "_" + to_string(j) + ".csv";
        remove(filename.c_str());
      }
    }
  }

  // Then delete the matrix object and remove from catalogue
  delete this->matrices[matrixName];
  this->matrices.erase(matrixName);
}

Matrix *MatrixCatalogue::getMatrix(string matrixName)
{
  logger.log("MatrixCatalogue::getMatrix");
  Matrix *matrix = this->matrices[matrixName];
  return matrix;
}

bool MatrixCatalogue::isMatrix(string matrixName)
{
  logger.log("MatrixCatalogue::isMatrix");
  if (this->matrices.count(matrixName))
    return true;
  return false;
}

void MatrixCatalogue::print()
{
  logger.log("MatrixCatalogue::print");
  cout << "\nMATRICES" << endl;

  int rowCount = 0;
  for (auto matrix : this->matrices)
  {
    cout << matrix.first << " (Size: " << matrix.second->matrixSize
         << "x" << matrix.second->matrixSize
         << ", Block size: " << matrix.second->blockSize << ")" << endl;
    rowCount++;
  }
  printRowCount(rowCount);
}

MatrixCatalogue::~MatrixCatalogue()
{
  logger.log("MatrixCatalogue::~MatrixCatalogue");

  // Clean up all matrices and their temporary files
  for (auto matrix : this->matrices)
  {
    int noOfBlocks = matrix.second->noOfBlocksInRow;
    for (int i = 0; i < noOfBlocks; i++)
    {
      for (int j = 0; j < noOfBlocks; j++)
      {
        string filename = "../data/temp/" + matrix.first + "_" + to_string(i) + "_" + to_string(j) + ".csv";
        remove(filename.c_str());
      }
    }
    delete matrix.second;
  }
}
