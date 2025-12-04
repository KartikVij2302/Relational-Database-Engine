#ifndef MATRIX_H
#define MATRIX_H

#include "global.h"
#include <fstream>
#include <limits>

class Matrix
{
private:
  static const int BLOCK_SIZE_KB = 1; // Block size in KB
  static const int BYTES_PER_BLOCK = BLOCK_SIZE_KB * 1024;

  // Helper functions
  int calculateBlockSize();
  void calculateBlocksInRow();
  void saveBlock(const vector<vector<int>> &block, const string &filename);

public:
  string name;
  int matrixSize;      // Size of the square matrix (n x n)
  int blockSize;       // Number of rows/columns in each block
  int noOfBlocksInRow; // Number of blocks in each row/column



  // Constructor
  Matrix(const string &matrixName);

  // Core functions
  bool loadAndPartition(const string &inputFilePath);
  vector<vector<int>> getBlock(int blockRow, int blockCol) const;
  bool print();
  bool rotate();
  bool exportMatrix();
  bool checkAntiSymmetric(const Matrix &other);
  bool crossTranspose(const Matrix &other);
  bool groupBy();
};

#endif // MATRIX_H  // End of include guard
