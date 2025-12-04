#include "global.h"
#include "externalsort.h"
/**
 * @brief 
 * SYNTAX: R <- JOIN relation_name1, relation_name2 ON column_name1 , column_name2
 */
bool syntacticParseJOIN()
{
    logger.log("syntacticParseJOIN");
    if (tokenizedQuery.size() != 8 || tokenizedQuery[5] != "ON")
    {
        cout << "SYNTAC ERROR" << endl;
        cout<< "tokenizedQuery.size() "<<tokenizedQuery.size()<<endl;
        return false;
    }
    parsedQuery.queryType = JOIN;
    parsedQuery.joinResultRelationName = tokenizedQuery[0];
    parsedQuery.joinFirstRelationName = tokenizedQuery[3];
    parsedQuery.joinSecondRelationName = tokenizedQuery[4];
    parsedQuery.joinFirstColumnName = tokenizedQuery[6];
    parsedQuery.joinSecondColumnName = tokenizedQuery[7];

    // string binaryOperator = tokenizedQuery[7];
    // if (binaryOperator == "<")
    //     parsedQuery.joinBinaryOperator = LESS_THAN;
    // else if (binaryOperator == ">")
    //     parsedQuery.joinBinaryOperator = GREATER_THAN;
    // else if (binaryOperator == ">=" || binaryOperator == "=>")
    //     parsedQuery.joinBinaryOperator = GEQ;
    // else if (binaryOperator == "<=" || binaryOperator == "=<")
    //     parsedQuery.joinBinaryOperator = LEQ;
    // else if (binaryOperator == "==")
    //     parsedQuery.joinBinaryOperator = EQUAL;
    // else if (binaryOperator == "!=")
    //     parsedQuery.joinBinaryOperator = NOT_EQUAL;
    // else
    // {
    //     cout << "SYNTAX ERROR" << endl;
    //     return false;
    // }
    return true;
}

bool semanticParseJOIN()
{
    logger.log("semanticParseJOIN");

    if (tableCatalogue.isTable(parsedQuery.joinResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.joinFirstRelationName) || !tableCatalogue.isTable(parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.joinFirstColumnName, parsedQuery.joinFirstRelationName) || !tableCatalogue.isColumnFromTable(parsedQuery.joinSecondColumnName, parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }
    return true;
}


int hashFunction(int key, int num_of_partitions) {
    return hash<int>{}(key) % num_of_partitions;
}

void writeRowsToCSV(const string &filename, const vector<vector<int>> &rows) {
    ofstream file(filename, ios::out | ios::app);
    if (!file.is_open()) {
        logger.log("Error opening file: " + filename);
        return;
    }
    logger.log("Writing to file: " + filename);
    for (const auto &row : rows) {
        for (size_t i = 0; i < row.size(); ++i) {
            file << row[i];
            if (i < row.size() - 1) file << ",";
        }
        file << "\n";
    }
    file.close();
}

vector<vector<int>> readCSV(const string &filename) {
    vector<vector<int>> data;
    ifstream file(filename);
    string line;
    while (getline(file, line)) {
        stringstream ss(line);
        vector<int> row;
        string cell;
        while (getline(ss, cell, ',')) {
            row.push_back(stoi(cell));
        }
        data.push_back(row);
    }
    file.close();
    return data;
}



void executeJOIN() {
    logger.log("executeJOIN");

    int num_of_partitions = 10; 

    Table *table1 = tableCatalogue.getTable(parsedQuery.joinFirstRelationName);
    Table *table2 = tableCatalogue.getTable(parsedQuery.joinSecondRelationName);

    vector<string> columnNames = table1->columns;
    columnNames.insert(columnNames.end(), table2->columns.begin(), table2->columns.end());

    Table *resultantTable = new Table(parsedQuery.joinResultRelationName, columnNames);
    columnNames.clear();

    int maxRowsPerBlock1 = table1->maxRowsPerBlock;
    int maxRowsPerBlock2 = table2->maxRowsPerBlock;

    vector<int> partitionFileCounters(num_of_partitions, 0);
    vector<vector<string>> table1PartitionFiles(num_of_partitions);
    vector<vector<string>> table2PartitionFiles(num_of_partitions);

    unordered_map<int, vector<vector<int>>> partitionBuffers1[num_of_partitions];
    unordered_map<int, vector<vector<int>>> partitionBuffers2[num_of_partitions];

    int column1Index = table1->getColumnIndex(parsedQuery.joinFirstColumnName);
    int column2Index = table2->getColumnIndex(parsedQuery.joinSecondColumnName);

    // Partitioning phase for table1
    Cursor cursor1 = table1->getCursor();
    while (true) {
        vector<int> row = cursor1.getNext();
        if (row.empty()) break;

        int key = row[column1Index];
        int partition = hashFunction(key, num_of_partitions);
        partitionBuffers1[partition][key].push_back(row);

        if (partitionBuffers1[partition][key].size() >= maxRowsPerBlock1) {
            logger.log("Writing partition1_" + to_string(partition) + "_" + to_string(partitionFileCounters[partition]));
            string filename = "../data/temp/partition1_" + to_string(partition) + "_" + to_string(partitionFileCounters[partition]++) + ".csv";
            table1PartitionFiles[partition].push_back(filename);
            writeRowsToCSV(filename, partitionBuffers1[partition][key]);
            partitionBuffers1[partition][key].clear();
        }
    }

    // Partitioning phase for table2
    Cursor cursor2 = table2->getCursor();
    while (true) {
        vector<int> row = cursor2.getNext();
        if (row.empty()) break;

        int key = row[column2Index];
        int partition = hashFunction(key, num_of_partitions);
        partitionBuffers2[partition][key].push_back(row);

        if (partitionBuffers2[partition][key].size() >= maxRowsPerBlock2) {
            string filename = "../data/temp/partition2_" + to_string(partition) + "_" + to_string(partitionFileCounters[partition]++) + ".csv";
            table2PartitionFiles[partition].push_back(filename);
            writeRowsToCSV(filename, partitionBuffers2[partition][key]);
            partitionBuffers2[partition][key].clear();
        }
    }

    for (int p = 0; p < num_of_partitions; ++p) {
        for (auto &buffer : partitionBuffers1[p]) {
            if (!buffer.second.empty()) {
                logger.log("Writing partition1_" + to_string(p) + "_" + to_string(partitionFileCounters[p]));
                string filename = "../data/temp/partition1_" + to_string(p) + "_" + to_string(partitionFileCounters[p]++) + ".csv";
                table1PartitionFiles[p].push_back(filename);
                writeRowsToCSV(filename, buffer.second);
            }
        }
    }

    for (int p = 0; p < num_of_partitions; ++p) {
        for (auto &buffer : partitionBuffers2[p]) {
            if (!buffer.second.empty()) {
                string filename = "../data/temp/partition2_" + to_string(p) + "_" + to_string(partitionFileCounters[p]++) + ".csv";
                table2PartitionFiles[p].push_back(filename);
                writeRowsToCSV(filename, buffer.second);
            }
        }
    }

    // Clear partition buffers
    for (int p = 0; p < num_of_partitions; ++p) {
        partitionBuffers1[p].clear();
        partitionBuffers2[p].clear();
    }

    for (int p = 0; p < num_of_partitions; ++p) {
        if (table1PartitionFiles[p].empty() || table2PartitionFiles[p].empty()) continue;
        for (const auto &file1 : table1PartitionFiles[p]) {
            // Read one block at a time from table1
            vector<vector<int>> blockTable1 = readCSV(file1);
            
            // Create a hash table for this block
            unordered_map<int, vector<vector<int>>> blockHashTable;
            for (const auto &row : blockTable1) {
                int key = row[column1Index];
                blockHashTable[key].push_back(row);
            }

            // Process each block from table2 in the current partition
            for (const auto &file2 : table2PartitionFiles[p]) 
            {
                vector<vector<int>> blockTable2 = readCSV(file2);
                
                for (const auto &row2 : blockTable2) 
                {
                    int key = row2[column2Index];
                    
                    // Check if key exists in the current block's hash table and join
                    if (blockHashTable.find(key) != blockHashTable.end()) 
                    {
                        for (auto &row1 : blockHashTable[key]) 
                        {
                            vector<int> newRow = row1;
                            newRow.insert(newRow.end(), row2.begin(), row2.end());
                            resultantTable->writeRow<int>(newRow);
                        }
                    }
                }
            }
        }
    }
    
    // Delete temporary files
    for (int p = 0; p < num_of_partitions; ++p) {
        for (const auto &file : table1PartitionFiles[p]) {
            if (remove(file.c_str()) != 0) {
                logger.log("Error deleting temporary file: " + file);
            }
        }

        for (const auto &file : table2PartitionFiles[p]) 
        {
            if (remove(file.c_str()) != 0) 
            {
                logger.log("Error deleting temporary file: " + file);
            }
        }
    }
    
    resultantTable->blockify();
    tableCatalogue.insertTable(resultantTable);
    // Sort the resultant table
    ExternalSort externalsort(resultantTable,{parsedQuery.joinFirstColumnName,parsedQuery.joinSecondColumnName},{true,true});
    externalsort.performExternalSort();
}



