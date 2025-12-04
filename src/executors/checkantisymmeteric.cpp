#include "matrix.h"
#include <fstream>
#include "matrixCatalogue.h"
/**
 * @brief
 * SYNTAX: CHECKANTISYM matrix_name
 */

bool syntacticParseCheckAntiSymmetric()
{
    logger.log("syntacticParseCheckAntiSymmetric");
    
    if (tokenizedQuery.size() != 3)
    {
        cout << "SYNTAX ERROR: Invalid number of arguments." << endl;
        return false;
    }
    
    parsedQuery.queryType = CHECKANTISYM;
    parsedQuery.checkAntiSymmetricMatrixName1 = tokenizedQuery[1];
    parsedQuery.checkAntiSymmetricMatrixName2 = tokenizedQuery[2];
    return true;
}

bool semanticParseCheckAntiSymmetric()
{
    logger.log("semanticParseCheckAntiSymmetric");
    
    string filePath1 = "../data/" + parsedQuery.checkAntiSymmetricMatrixName1 ;
    string filePath2 = "../data/" + parsedQuery.checkAntiSymmetricMatrixName2 ;
    
    if (isFileExists(filePath1) && isFileExists(filePath2))
    {
        return true;
    }
    
    cout << "SEMANTIC ERROR: One or both matrix files do not exist." << endl;
    return false;
}

void executeCheckAntiSymmetric()
{
    logger.log("executeCheckAntiSymmetric");
    
    // Create Matrix objects
    Matrix *matrix1 = matrixCatalogue.getMatrix(parsedQuery.checkAntiSymmetricMatrixName1);
    Matrix *matrix2 = matrixCatalogue.getMatrix(parsedQuery.checkAntiSymmetricMatrixName2);
    // Check if matrices have same dimensions
    if (matrix1->matrixSize != matrix2->matrixSize)
    {
        cout << "ERROR: Matrices have different dimensions." << endl;
        return;
    }
    
    // Perform anti-symmetric check using block-based approach
    bool isAntiSymmetric = matrix1->checkAntiSymmetric(*matrix2);
    
    cout << (isAntiSymmetric ? "True" : "False") << endl;
}