#include "global.h"

// enum QueryType { GROUPBY };

/**
 * @brief
 * SYNTAX: Result <- GROUP BY <attribute1> FROM <table> 
 *         HAVING <Aggregate-Func1(attribute2)> <bin-op> <value> 
 *         RETURN <Aggregate-Func2(attribute3)>
 */

bool syntacticParseGROUPBY() {
    logger.log("syntacticParseGROUPBY");

    // Expected token order for:
    // Result <- GROUP BY DepartmentID FROM Groupby HAVING AVG(Salary) > 50000 RETURN MAX(Salary)
    // Token indices:
    //  0: Result
    //  1: <- 
    //  2: GROUP
    //  3: BY
    //  4: DepartmentID
    //  5: FROM
    //  6: Groupby
    //  7: HAVING
    //  8: AVG(Salary)
    //  9: >
    // 10: 50000
    // 11: RETURN
    // 12: MAX(Salary)
    if (tokenizedQuery.size() != 13 || tokenizedQuery[1] != "<-" ||
        tokenizedQuery[2] != "GROUP" || tokenizedQuery[3] != "BY" ||
        tokenizedQuery[5] != "FROM" || tokenizedQuery[7] != "HAVING" ||
        tokenizedQuery[11] != "RETURN") 
    {
        cout << "SYNTAX ERROR: Invalid GROUP BY query format." << endl;
        return false;
    }

    parsedQuery.queryType = GROUPBY;

    // Extract basic components
    parsedQuery.groupByResultRelationName = tokenizedQuery[0];  // Result table name
    parsedQuery.groupByAttribute = tokenizedQuery[4];           // Attribute to group by
    parsedQuery.groupByTableName = tokenizedQuery[6];           // Source table name

    // Parse HAVING clause (token 8, 9, 10)
    string havingClause = tokenizedQuery[8];  // e.g., "AVG(Salary)"
    size_t openParen = havingClause.find('(');
    size_t closeParen = havingClause.find(')');
    if (openParen == string::npos || closeParen == string::npos || closeParen <= openParen + 1) {
        cout << "SYNTAX ERROR: Invalid HAVING clause format." << endl;
        return false;
    }
    parsedQuery.havingAggregateFunc = havingClause.substr(0, openParen);  // e.g., "AVG"
    parsedQuery.havingAttribute = havingClause.substr(openParen + 1, closeParen - openParen - 1);  // e.g., "Salary"
    
    parsedQuery.havingOperator = tokenizedQuery[9];  // e.g., ">"
    try {
        parsedQuery.havingValue = stoi(tokenizedQuery[10]);  // e.g., 50000
    } catch (invalid_argument &e) {
        cout << "SYNTAX ERROR: Invalid HAVING value." << endl;
        return false;
    }

    // Parse RETURN clause (token 12)
    string returnClause = tokenizedQuery[12];  // e.g., "MAX(Salary)"
    openParen = returnClause.find('(');
    closeParen = returnClause.find(')');
    if (openParen == string::npos || closeParen == string::npos || closeParen <= openParen + 1) {
        cout << "SYNTAX ERROR: Invalid RETURN clause format." << endl;
        return false;
    }
    parsedQuery.returnAggregateFunc = returnClause.substr(0, openParen);  // e.g., "MAX"
    parsedQuery.returnAttribute = returnClause.substr(openParen + 1, closeParen - openParen - 1);  // e.g., "Salary"

    return true;
}

bool semanticParseGroupBy() {
    logger.log("semanticParseGROUPBY");

    if (tableCatalogue.isTable(parsedQuery.groupByResultRelationName)) {
        cout << "SEMANTIC ERROR: Resultant table already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.groupByTableName)) {
        cout << "SEMANTIC ERROR: Table doesn't exist" << endl;
        return false;
    }

    Table *table = tableCatalogue.getTable(parsedQuery.groupByTableName);

    if (!table->isColumn(parsedQuery.groupByAttribute) ||
        !table->isColumn(parsedQuery.havingAttribute)) {
        cout << "SEMANTIC ERROR: Attribute doesn't exist in the table" << endl;
        return false;
    }

    return true;
}
#include <unordered_map>
#include <functional>
#include <numeric>
#include <cmath>
#include "matrix.h"
#include "matrixCatalogue.h"
#include "global.h"
#include "bits/stdc++.h"
using namespace std;

bool compare(int val1, const string &op, int val2) {
    if (op == "==") return val1 == val2;
    if (op == "!=") return val1 != val2;
    if (op == "<")  return val1 < val2;
    if (op == "<=") return val1 <= val2;
    if (op == ">")  return val1 > val2;
    if (op == ">=") return val1 >= val2;
    return false;
}

// Structure to hold intermediate aggregate values per group.
struct AggData {
    long long sum_having;
    long long sum_return;
    int count;
    int min_having;
    int max_having;
    int min_return;
    int max_return;
    bool init;
    AggData() : sum_having(0), sum_return(0), count(0),
                min_having(numeric_limits<int>::max()), max_having(numeric_limits<int>::min()),
                min_return(numeric_limits<int>::max()), max_return(numeric_limits<int>::min()),
                init(false) {}
};


bool compareRows(const vector<int>& row1, const vector<int>& row2) {
    return row1[0] < row2[0]; // Sort based on the first attribute (DepartmentID)
}


#include "global.h"

/**
 * @brief Executes the GROUP BY query with HAVING clause using 10 blocks at a time.
 */
void executeGroupBy() {
    logger.log("executeGROUPBY");

    // Configuration for external memory processing
    const int MAX_BLOCKS = 10;             // Limit memory usage to 10 blocks
    const int NUM_PARTITIONS = 10;         // Number of partitions

    // Get the table
    Table *table = tableCatalogue.getTable(parsedQuery.groupByTableName);
    int groupByIndex = table->getColumnIndex(parsedQuery.groupByAttribute);
    int havingIndex = table->getColumnIndex(parsedQuery.havingAttribute);
    int returnIndex = table->getColumnIndex(parsedQuery.returnAttribute);

    // Partition management
    vector<int> partitionFileCounters(NUM_PARTITIONS, 0);
    vector<vector<string>> partitionFiles(NUM_PARTITIONS);

    // First Pass: Partition the data in 10-block batches
    Cursor cursor = table->getCursor();
    while (true) {
        vector<vector<int>> batchRows;

        // Read the next batch of 10 blocks
        for (int i = 0; i < MAX_BLOCKS; ++i) {
            vector<int> row = cursor.getNext();
            if (row.empty()) break;
            batchRows.push_back(row);
        }

        if (batchRows.empty()) break;

        // Partitioning phase
        unordered_map<int, vector<vector<int>>> partitionBuffers[NUM_PARTITIONS];

        for (const auto &row : batchRows) {
            int key = row[groupByIndex];
            int partition = std::abs(static_cast<int>(hash<int>{}(key) % NUM_PARTITIONS));

            // Add row to partition buffer
            partitionBuffers[partition][key].push_back(row);

            // Write to disk when buffer limit reaches table->maxRowsPerBlock
            if (partitionBuffers[partition][key].size() >= table->maxRowsPerBlock) {
                string filename = "../data/temp/groupby_" + to_string(partition) + 
                                  "_" + to_string(partitionFileCounters[partition]++) + ".csv";
                partitionFiles[partition].push_back(filename);

                ofstream file(filename, ios::out | ios::app);
                for (const auto &groupRow : partitionBuffers[partition][key]) {
                    for (size_t i = 0; i < groupRow.size(); ++i) {
                        file << groupRow[i];
                        if (i < groupRow.size() - 1) file << ",";
                    }
                    file << "\n";
                }
                file.close();

                // Clear the buffer
                partitionBuffers[partition][key].clear();
            }
        }

        // Flush remaining buffers
        for (int p = 0; p < NUM_PARTITIONS; ++p) {
            for (auto &buffer : partitionBuffers[p]) {
                if (!buffer.second.empty()) {
                    string filename = "../data/temp/groupby_" + to_string(p) + 
                                      "_" + to_string(partitionFileCounters[p]++) + ".csv";
                    partitionFiles[p].push_back(filename);

                    ofstream file(filename, ios::out | ios::app);
                    for (const auto &groupRow : buffer.second) {
                        for (size_t i = 0; i < groupRow.size(); ++i) {
                            file << groupRow[i];
                            if (i < groupRow.size() - 1) file << ",";
                        }
                        file << "\n";
                    }
                    file.close();
                }
            }
        }
    }

    // Prepare result storage
    vector<vector<int>> resultRows;

    // Second Pass: Process each partition within memory constraints
    for (int p = 0; p < NUM_PARTITIONS; ++p) {
        if (partitionFiles[p].empty()) continue;

        // For each partition, process files in blocks
        for (const auto &file : partitionFiles[p]) {
            // Read file contents
            ifstream inputFile(file);
            vector<vector<int>> partitionData;
            string line;
            while (getline(inputFile, line)) {
                vector<int> row;
                stringstream ss(line);
                string cell;
                while (getline(ss, cell, ',')) {
                    row.push_back(stoi(cell));
                }
                partitionData.push_back(row);
            }
            inputFile.close();

            // Group and aggregate within this partition
            unordered_map<int, vector<vector<int>>> groups;
            for (const auto &row : partitionData) {
                int key = row[groupByIndex];
                groups[key].push_back(row);
            }

            // Process each group
            for (auto &group : groups) {
                int groupKey = group.first;
                vector<vector<int>> &rows = group.second;

                // Compute HAVING aggregate
                int havingAggValue = 0;
                if (parsedQuery.havingAggregateFunc == "SUM") {
                    havingAggValue = accumulate(rows.begin(), rows.end(), 0,
                        [&](int sum, const vector<int> &row) { return sum + row[havingIndex]; });
                } else if (parsedQuery.havingAggregateFunc == "COUNT") {
                    havingAggValue = rows.size();
                } else if (parsedQuery.havingAggregateFunc == "AVG") {
                    havingAggValue = accumulate(rows.begin(), rows.end(), 0,
                        [&](int sum, const vector<int> &row) { return sum + row[havingIndex]; }) / rows.size();
                } else if (parsedQuery.havingAggregateFunc == "MAX") {
                    havingAggValue = numeric_limits<int>::min();
                    for (const auto &row : rows)
                        havingAggValue = max(havingAggValue, row[havingIndex]);
                } else if (parsedQuery.havingAggregateFunc == "MIN") {
                    havingAggValue = numeric_limits<int>::max();
                    for (const auto &row : rows)
                        havingAggValue = min(havingAggValue, row[havingIndex]);
                }

                // Apply HAVING condition
                if (!compare(havingAggValue, parsedQuery.havingOperator, parsedQuery.havingValue))
                    continue;

                // Compute RETURN aggregate
                int returnAggValue = 0;
                if (parsedQuery.returnAggregateFunc == "SUM") {
                    returnAggValue = accumulate(rows.begin(), rows.end(), 0,
                        [&](int sum, const vector<int> &row) { return sum + row[returnIndex]; });
                } else if (parsedQuery.returnAggregateFunc == "COUNT") {
                    returnAggValue = rows.size();
                } else if (parsedQuery.returnAggregateFunc == "AVG") {
                    returnAggValue = accumulate(rows.begin(), rows.end(), 0,
                        [&](int sum, const vector<int> &row) { return sum + row[returnIndex]; }) / rows.size();
                } else if (parsedQuery.returnAggregateFunc == "MAX") {
                    returnAggValue = numeric_limits<int>::min();
                    for (const auto &row : rows)
                        returnAggValue = max(returnAggValue, row[returnIndex]);
                } else if (parsedQuery.returnAggregateFunc == "MIN") {
                    returnAggValue = numeric_limits<int>::max();
                    for (const auto &row : rows)
                        returnAggValue = min(returnAggValue, row[returnIndex]);
                }

                // Store result row
                resultRows.push_back({groupKey, returnAggValue});
            }
        }
    }

    // Sort the result rows on the groupBy attribute (first column)
    sort(resultRows.begin(), resultRows.end(), compareRows);

    // Create the resultant table with proper column names
    vector<string> columnNames = { 
        parsedQuery.groupByAttribute, 
        parsedQuery.returnAggregateFunc + "(" + parsedQuery.returnAttribute + ")" 
    };
    Table *resultantTable = new Table(parsedQuery.groupByResultRelationName, columnNames);

    for (const auto &row : resultRows) {
        resultantTable->writeRow<int>(row);
    }
    resultantTable->blockify();
    tableCatalogue.insertTable(resultantTable);
}
