// write a code to generate a matrix of 300*300

#include <iostream>
#include <fstream>

using namespace std;

int main() {
    ofstream file("matrix_300x300.csv"); // Open file for writing

    if (!file) {
        cout << "Error opening file!" << endl;
        return 1;
    }

    int n = 300; // Matrix size (300 x 300)
    
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
            file << (i * n + j + 1); // Sequential values from 1 to 90000
            
            if (j < n - 1) file << ","; // Add comma except at row end
        }
        file << "\n"; // New line after each row
    }

    file.close();
    cout << "Matrix generated and saved to matrix_300x300.csv" << endl;

    return 0;
}
