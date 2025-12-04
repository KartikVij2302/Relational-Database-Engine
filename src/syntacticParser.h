#include "tableCatalogue.h"
using namespace std;

enum QueryType
{
  CLEAR,
  CROSS,
  DISTINCT,
  EXPORT,
  INDEX,
  JOIN,
  LIST,
  LOAD,
  PRINT,
  PROJECTION,
  RENAME,
  SELECTION,
  SORT,
  SOURCE,
  LOAD_MATRIX, // Added LOAD_MATRIX
  PRINT_MATRIX,
  ROTATE_MATRIX,  // Added ROTATE_MATRIX
  CHECKANTISYM,   // Added CHECKANTISYM
  EXPORT_MATRIX,  // Added EXPORT_MATRIX
  CROSSTRANSPOSE, // Added CROSSTRANSPOSE
  GROUPBY,
  UNDETERMINED,
  ORDERBY,
  SEARCH,
  DELETE,
  INSERT,
UPDATE,

};

enum BinaryOperator
{
    LESS_THAN,
    GREATER_THAN,
    LEQ,
    GEQ,
    EQUAL,
    NOT_EQUAL,
    NO_BINOP_CLAUSE
};

enum SortingStrategy
{
    ASC,
    DESC,
    NO_SORT_CLAUSE
};

enum SelectType
{
    COLUMN,
    INT_LITERAL,
    NO_SELECT_CLAUSE
};

class ParsedQuery
{
public:
    QueryType queryType = UNDETERMINED;

    string clearRelationName = "";
    string crossResultRelationName = "";
    string crossFirstRelationName = "";
    string crossSecondRelationName = "";
    string distinctResultRelationName = "";
    string distinctRelationName = "";
    string exportRelationName = "";
    IndexingStrategy indexingStrategy = NOTHING;
    string indexColumnName = "";
    string indexRelationName = "";
    BinaryOperator joinBinaryOperator = NO_BINOP_CLAUSE;
    string joinResultRelationName = "";
    string joinFirstRelationName = "";
    string joinSecondRelationName = "";
    string joinFirstColumnName = "";
    string joinSecondColumnName = "";
    string loadRelationName = "";
    string printRelationName = "";
    string projectionResultRelationName = "";
    vector<string> projectionColumnList;
    string projectionRelationName = "";
    string renameFromColumnName = "";
    string renameToColumnName = "";
    string renameRelationName = "";
    SelectType selectType = NO_SELECT_CLAUSE;
    BinaryOperator selectionBinaryOperator = NO_BINOP_CLAUSE;
    string selectionResultRelationName = "";
    string selectionRelationName = "";
    string selectionFirstColumnName = "";
    string selectionSecondColumnName = "";
    int selectionIntLiteral = 0;
    SortingStrategy sortingStrategy = NO_SORT_CLAUSE;
    string sortResultRelationName = "";
    string sortColumnName = "";
    string sortRelationName = "";
    string sourceFileName = "";
    string insertRelationName = "";
    vector<string> insertColumnNames;
    vector<int> insertValues;

    
    string loadMatrixName = ""; // Added loadMatrixName
    string printMatrixName = ""; // Added printMatrixName
    string exportMatrixName = ""; // Added exportMatrixName
    string crossTransposeMatrixName1 = ""; // Added crossTransposeMatrixName
    string crossTransposeMatrixName2 = ""; // Added crossTransposeMatrixName
    string rotateMatrixName = ""; // Added rotateMatrixName
    string checkAntiSymmetricMatrixName1 = ""; // Added checkAntiSymmetricMatrixName
    string checkAntiSymmetricMatrixName2 = ""; // Added checkAntiSymmetricMatrixName
    vector<string> sortColumnNames;
    vector<bool> sortingDirection; // true for ASC, false for DESC
    string groupByResultRelationName = "";
    string groupByAttribute = "";
    string groupByTableName = "";
    string havingAggregateFunc = "";
    string havingAttribute = "";
    string havingOperator = "";
    int havingValue = 0;
    string returnAggregateFunc = "";
    string returnAttribute = "";
    string havingRelationName = "";
    string returnRelationName = "";
    string havingColumnName = "";
    string returnColumnName = "";
    string havingClause = "";
    string returnClause = "";
    string groupByRelationName = "";
    string resultRelationName ="";

    string searchResultRelationName="";
    string searchRelationName = "";
    string searchColumnName = "";
    string searchOperator="";
    string deleteRelationName = "";
    string deleteColumnName = "";
    string deleteOperator = "";
    int deleteValue = 0;
    int searchValue = 0;
    string updateValueClauseValue = "";
    string updateConditionColumnName = "";
    string updateConditionOperator = "";
    int updateConditionValue = 0;
    string updateTargetColumnName = "";
    int updateTargetValue = 0;
    string updateTargetColumnClause = "";
    string updateTargetValueClause = "";
    string updateConditionColumnClause = "";
    string updateConditionValueClause = "";
    string updateConditionOperatorClause = "";
    string updateRelationName = "";
    string updateClause = "";




    ParsedQuery();
    void clear();
};

bool syntacticParse();
bool syntacticParseCLEAR();
bool syntacticParseCROSS();
bool syntacticParseDISTINCT();
bool syntacticParseEXPORT();
bool syntacticParseINDEX();
bool syntacticParseJOIN();
bool syntacticParseLIST();
bool syntacticParseLOAD();
bool syntacticParsePRINT();
bool syntacticParsePROJECTION();
bool syntacticParseRENAME();
bool syntacticParseSELECTION();
bool syntacticParseSORT();
bool syntacticParseSOURCE();
bool syntacticParseORDERBY();
bool syntacticParseINSERT();

bool isFileExists(string tableName);
bool isQueryFile(string fileName);

// Matrix specific syntactic parsers
bool syntacticParseLOADMATRIX();
bool syntacticParsePRINTMATRIX();
bool syntacticParseCheckAntiSymmetric();
bool syntacticParseROTATEMATRIX();
bool syntacticParseEXPORTMATRIX();
bool syntacticParseCROSSTRANSPOSEMATRIX();
bool syntacticParseGROUPBY();
bool syntacticParseSEARCH();
bool syntacticParseDELETE();
bool syntacticParseUPDATE();
