#include"global.h"

bool semanticParse(){
    logger.log("semanticParse");
    switch(parsedQuery.queryType){
        case CLEAR: return semanticParseCLEAR();
        case CROSS: return semanticParseCROSS();
        case DISTINCT: return semanticParseDISTINCT();
        case EXPORT: return semanticParseEXPORT();
        case INDEX: return semanticParseINDEX();
        case JOIN: return semanticParseJOIN();
        case LIST: return semanticParseLIST();
        case LOAD: return semanticParseLOAD();
        case PRINT: return semanticParsePRINT();
        case PROJECTION: return semanticParsePROJECTION();
        case RENAME: return semanticParseRENAME();
        case SELECTION: return semanticParseSELECTION();
        case SORT: return semanticParseSORT();
        case SOURCE: return semanticParseSOURCE();
        case LOAD_MATRIX: return semanticParseLOADMATRIX();
        case PRINT_MATRIX: return semanticParsePRINTMATRIX();
        case ROTATE_MATRIX: return semanticParseROTATEMATRIX();
        case CHECKANTISYM: return semanticParseCheckAntiSymmetric();
        case EXPORT_MATRIX: return semanticParseEXPORTMATRIX();
        case CROSSTRANSPOSE: return semanticParseCROSSTRANSPOSEMATRIX();
        case GROUPBY: return semanticParseGroupBy();
        case ORDERBY: return semanticParseORDERBY();
        case SEARCH: return semanticParseSEARCH();
        case DELETE: return semanticParseDELETE();
        case INSERT: return semanticParseINSERT();
        case UPDATE: return semanticParseUPDATE();
        default: cout<<"SEMANTIC ERROR"<<endl;
    }

    return false;
}