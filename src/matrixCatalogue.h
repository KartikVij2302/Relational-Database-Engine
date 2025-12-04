#ifndef MATRIX_CATALOGUE_H
#define MATRIX_CATALOGUE_H

#include "global.h"
#include "matrix.h"


/**
 * @brief The MatrixCatalogue acts like an index of tables existing in the
 * system. Everytime a matrix is added(removed) to(from) the system, it needs to
 * be added(removed) to(from) the matrixCatalogue.
 *
 */
// #include <unordered_map>

class MatrixCatalogue
{
private:
  unordered_map<string, Matrix *> matrices;

public:
  // Constructor and Destructor
  MatrixCatalogue() {}
  ~MatrixCatalogue();

  // Matrix management functions
  void insertMatrix(Matrix *matrix);
  void deleteMatrix(string matrixName);
  Matrix *getMatrix(string matrixName);
  bool isMatrix(string matrixName);

  // Utility functions
  void print();
};

extern MatrixCatalogue matrixCatalogue;
#endif // MATRIX_CATALOGUE_H
