#include "semanticParser.h"

void executeCommand();

void executeCLEAR();
void executeCROSS();
void executeDISTINCT();
void executeEXPORT();
void executeINDEX();
void executeJOIN();
void executeLIST();
void executeLOAD();
void executePRINT();
void executePROJECTION();
void executeRENAME();
void executeSELECTION();
void executeSORT();
void executeSOURCE();

void executeLOADMATRIX();
void executePRINTMATRIX();
void executeROTATEMATRIX();
void executeCheckAntiSymmetric();
void executeEXPORTMATRIX();
void executeCROSSTRANSPOSE();
void executeGroupBy();
void executeORDERBY();

bool evaluateBinOp(int value1, int value2, BinaryOperator binaryOperator);
void printRowCount(int rowCount);
void executeSEARCH();
void executeDELETE();
void executeINSERT();
void executeUPDATE();