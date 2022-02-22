/*
Skeleton code for linear hash indexing
*/

#include <ios>
#include <fstream>
#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <cmath>
#include "classes.h"
using namespace std;


int main(int argc, char* const argv[]) {

    // Create the index
    LinearHashIndex emp_index("EmployeeIndex");
    emp_index.createFromFile("Employee.csv");
    
    // Loop to lookup IDs until user is ready to quit
    // ASSUMES USER INPUT IS MOSTLY CORRECT (I.E USER
    // ENTERS A VALID INTEGER ID INSTEAD OF ID THAT
    // DOESN'T EXIST OR A STRING, CAN DO ONLY BASIC
    // ERROR HANDLING)
    do
    {
        cout << "\n*******Linear Hash Index ID Search********\n";
		cout << "Enter ID to search\n";
		cout << "Enter 0 or negative number to quit\n";
		cout << "******************************************\n";

        int user_input;
        cout << ">> ";
        cin >> user_input;

        // Validate user input
        while(cin.fail()) {

            cout << "Error: Invalid ID!\n";
            cin.clear();
            cin.ignore(256, '\n');
            cout << ">> ";
            cin >> user_input;

        }

        // Search or quit
        if (user_input > 0) {

            cout << "Searching for ID " << user_input << "...\n";

            Record targetRecord = emp_index.findRecordById(user_input);

            // Print record
            targetRecord.print();

        }
        else
            break;

    } while (true);
    
    return 0;
}
