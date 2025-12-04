#include "global.h"
#include "secondary_index.h"

/**
 * @brief Syntax: UPDATE <table_name> WHERE <column_name> <operator> <value> SET <col_name> = <value>
 *
 * Modify the existing table in-place. Update every record matching the condition.
 * If no record follows the condition, no updates are performed.
 */
bool syntacticParseUPDATE()
{
    logger.log("syntacticParseUPDATE");

    // Expected tokens: UPDATE table WHERE col op val SET col = val
    if (tokenizedQuery.size() != 10 ||
        tokenizedQuery[0] != "UPDATE" ||
        tokenizedQuery[2] != "WHERE" ||
        tokenizedQuery[6] != "SET" ||
        tokenizedQuery[8] != "=")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = UPDATE;
    parsedQuery.updateRelationName = tokenizedQuery[1];
    parsedQuery.updateConditionColumnName = tokenizedQuery[3];

    // Parse condition operator
    string op = tokenizedQuery[4];
    if (op == "<" || op == "<=" || op == ">" || op == ">=" || op == "==" || op == "!=")
        parsedQuery.updateConditionOperator = op;
    else
    {
        cout << "SYNTAX ERROR: Invalid operator" << endl;
        return false;
    }

    // Condition value
    parsedQuery.updateConditionValue = stoi(tokenizedQuery[5]);

    // Target column and new value
    parsedQuery.updateTargetColumnName = tokenizedQuery[7];
    parsedQuery.updateTargetValue = stoi(tokenizedQuery[9]);

    return true;
}

/**
 * @brief Semantic validation for the UPDATE query
 */
bool semanticParseUPDATE()
{
    logger.log("semanticParseUPDATE");

    // Check table exists
    if (!tableCatalogue.isTable(parsedQuery.updateRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    Table *table = tableCatalogue.getTable(parsedQuery.updateRelationName);

    // Check columns exist
    if (!table->isColumn(parsedQuery.updateConditionColumnName))
    {
        cout << "SEMANTIC ERROR: Condition column doesn't exist" << endl;
        return false;
    }
    if (!table->isColumn(parsedQuery.updateTargetColumnName))
    {
        cout << "SEMANTIC ERROR: Target column doesn't exist" << endl;
        return false;
    }

    // Log if secondary index exists on condition column
    string condIndexFile = "../data/indices/" + parsedQuery.updateRelationName + "_" + parsedQuery.updateConditionColumnName + "_Indexfile_0";
    ifstream condIdx(condIndexFile);
    if (!condIdx)
        logger.log("No secondary index on condition column; will linear-scan.");
    else
    {
        condIdx.close();
        logger.log("Secondary index found on condition column; will use indexed update.");
    }

    // Log if secondary index exists on target column (to rebuild later)
    string targetIndexFile = "../data/indices/" + parsedQuery.updateRelationName + "_" + parsedQuery.updateTargetColumnName + "_Indexfile_0";
    ifstream targetIdx(targetIndexFile);
    if (!targetIdx)
        logger.log("No secondary index on target column.");
    else
    {
        targetIdx.close();
        logger.log("Secondary index exists on target column; will rebuild after updates.");
    }

    return true;
}

/**
 * @brief Execute the UPDATE query
 */
void executeUPDATE()
{
    logger.log("executeUPDATE");

    string tableName = parsedQuery.updateRelationName;
    Table *table = tableCatalogue.getTable(tableName);
    int condColIdx = table->getColumnIndex(parsedQuery.updateConditionColumnName);
    int targetColIdx = table->getColumnIndex(parsedQuery.updateTargetColumnName);
    int condVal = parsedQuery.updateConditionValue;
    string condOp = parsedQuery.updateConditionOperator;
    int newVal = parsedQuery.updateTargetValue;

    // Check secondary index on condition column
    string condIndexFile = "../data/indices/" + tableName + "_" + parsedQuery.updateConditionColumnName + "_Indexfile_0";
    ifstream condIdxCheck(condIndexFile);
    bool condIndexExists = condIdxCheck.good();
    condIdxCheck.close();

    // Track updated rows count
    int updatedCount = 0;

    // Gather record locations to update
    vector<pair<int,int>> records;
    if (condIndexExists)
    {
        // Use index to find matching records
        SecondaryIndex idx(tableName, parsedQuery.updateConditionColumnName);
        if (condOp == "==")
            records = idx.search(condVal);
        else if (condOp == "<")
            records = idx.rangeSearch(INT_MIN, condVal - 1);
        else if (condOp == "<=")
            records = idx.rangeSearch(INT_MIN, condVal);
        else if (condOp == ">")
            records = idx.rangeSearch(condVal + 1, INT_MAX);
        else if (condOp == ">=")
            records = idx.rangeSearch(condVal, INT_MAX);
        else if (condOp == "!=")
        {
            auto low = idx.rangeSearch(INT_MIN, condVal - 1);
            auto high = idx.rangeSearch(condVal + 1, INT_MAX);
            records.insert(records.end(), low.begin(), low.end());
            records.insert(records.end(), high.begin(), high.end());
        }

        // Perform in-place updates
        for (auto &loc : records)
        {
            int pageNo = loc.first;
            int recNo  = loc.second;

            Page page = bufferManager.getPage(tableName, pageNo);
            vector<int> row = page.getRow(recNo);
            row[targetColIdx] = newVal;
            page.updateRow(recNo, row);
            // bufferManager.writePage(tableName, pageNo, page);
            bufferManager.writePage(
                tableName,
                pageNo,
                page.getRows(),
                page.getRowCount()
            );
            

            updatedCount++;
        }
    }
    else
    {
        // Linear scan: iterate through all pages
        int numPages = table->getNumPages();
        for (int p = 0; p < numPages; ++p)
        {
            Page page = bufferManager.getPage(tableName, p);
            int numRecs = page.getNumRecords();
            for (int r = 0; r < numRecs; ++r)
            {
                vector<int> row = page.getRow(r);
                bool match = false;
                int field = row[condColIdx];
                if (condOp == "==") match = (field == condVal);
                else if (condOp == "!=") match = (field != condVal);
                else if (condOp == "<")  match = (field < condVal);
                else if (condOp == "<=") match = (field <= condVal);
                else if (condOp == ">")  match = (field > condVal);
                else if (condOp == ">=") match = (field >= condVal);

                if (match)
                {
                    row[targetColIdx] = newVal;
                    page.updateRow(r, row);
                    updatedCount++;
                }
            }
            // bufferManager.writePage(tableName, p, page);
            bufferManager.writePage(
                tableName,
                p,
                page.getRows(),
                page.getRowCount()
            );
            
        }
    }

    // Rebuild secondary index on target column if it existed before
    string targetIndexFile = "../data/indices/" + tableName + "_" + parsedQuery.updateTargetColumnName + "_Indexfile_0";
    ifstream targetIdxCheck(targetIndexFile);
    if (targetIdxCheck.good())
    {
        targetIdxCheck.close();
        SecondaryIndex newIdx(tableName, parsedQuery.updateTargetColumnName);
        newIdx.createIndex();
    }

    // Clear buffer pool to flush all changes
    bufferManager.clearPool();

    // Print summary
    if (updatedCount > 0)
        cout << "Updated " << updatedCount << " rows in '" << tableName << "'." << endl;
    else
        cout << "No rows matched the condition; 0 rows updated." << endl;
}
