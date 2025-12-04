#include<matrix.h>
#include<matrixCatalogue.h>
/**
 * @brief 
 * SYNTAX: CROSSTRANSPOSE <matrix_name1> <matrix_name2> 
 */


bool syntacticParseCROSSTRANSPOSEMATRIX()
{
    logger.log("syntacticParseCROSSTRANSPOSEMATRIX");
    if (tokenizedQuery.size() != 3)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = CROSSTRANSPOSE;
    parsedQuery.crossTransposeMatrixName1 = tokenizedQuery[1]; // Assuming the matrix name is the second token
    parsedQuery.crossTransposeMatrixName2 = tokenizedQuery[2]; // Assuming the matrix name is the third token
    return true;
}
bool semanticParseCROSSTRANSPOSEMATRIX()
{
    logger.log("semanticParseCROSSTRANSPOSEMATRIX");
    string filePath1 = "../data/" + parsedQuery.crossTransposeMatrixName1; // No  .csv here
    string filePath2 = "../data/" + parsedQuery.crossTransposeMatrixName2; // No  .csv here
    if (isFileExists(filePath1) && isFileExists(filePath2))
    {
        return true;
    }
    cout << "SEMANTIC ERROR: Data file doesn't exist" << endl;
    return false;
}

void executeCROSSTRANSPOSE()
{
    logger.log("executeCROSSTRANSPOSE");
    Matrix *matrix1 = matrixCatalogue.getMatrix(parsedQuery.crossTransposeMatrixName1);
    Matrix *matrix2 = matrixCatalogue.getMatrix(parsedQuery.crossTransposeMatrixName2);
    if (matrix1->crossTranspose(*matrix2))
    {
        cout << "Cross Transposed Matrix." << endl;
    }
    else
    {
        cout << "ERROR: Failed to cross transpose matrix." << endl;
    }
}