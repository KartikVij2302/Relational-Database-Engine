#include "matrix.h"
#include "matrixCatalogue.h"
/**
 * @brief 
 * SYNTAX: EXPORT MATRIX <matrix_name> 
 */

bool syntacticParseEXPORTMATRIX()
{
    logger.log("syntacticParseEXPORTMATRIX");
    if (tokenizedQuery.size() != 3)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = EXPORT_MATRIX;
    parsedQuery.exportMatrixName = tokenizedQuery[2]; // Assuming the matrix name is the third token
    return true;
}

bool semanticParseEXPORTMATRIX()
{
    logger.log("semanticParseEXPORTMATRIX");
    string filePath = "../data/" + parsedQuery.exportMatrixName; // No  .csv here
    if (isFileExists(filePath))
    {
        return true;
    }
    cout << "SEMANTIC ERROR: Data file doesn't exist" << endl;
    return false;
}

void executeEXPORTMATRIX()
{
    logger.log("executeEXPORTMATRIX");
    Matrix *matrix = matrixCatalogue.getMatrix(parsedQuery.exportMatrixName);
    if (matrix->exportMatrix())
    {
        cout << "Exported Matrix." << endl;
    }
    else
    {
        cout << "ERROR: Failed to export matrix." << endl;
    }
}