#ifndef GLOBAL_H
#define GLOBAL_H

#include"executor.h"
// #include "matrixCatalogue.h"

extern float BLOCK_SIZE;
extern uint BLOCK_COUNT;
extern uint PRINT_COUNT;
extern vector<string> tokenizedQuery;
extern ParsedQuery parsedQuery;
extern TableCatalogue tableCatalogue;
// extern MatrixCatalogue matrixCatalogue;
extern BufferManager bufferManager;
void doCommand();

#endif // GLOBAL_H
