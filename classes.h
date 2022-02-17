#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <bitset>
#include <fstream>
#include <cmath>
#include <cstdio>
using namespace std;

class Record {
public:
    int id, manager_id;
    std::string bio, name;

    Record(vector<std::string> fields) {
        id = stoi(fields[0]);
        name = fields[1];
        bio = fields[2];
        manager_id = stoi(fields[3]);
    }

    void print() {
        cout << "\tID: " << id << "\n";
        cout << "\tNAME: " << name << "\n";
        cout << "\tBIO: " << bio << "\n";
        cout << "\tMANAGER_ID: " << manager_id << "\n";
    }

    // Calculate size of record to determine if it can fit in block
    int calcSize() {

        // id and manager_id are both fixed 8 bytes
        // bio and name size depend on length (variable size)
        return 8 + 8 + bio.length() + name.length();

    }

    // Takes output stream and writes all member variables (not member functions) to it.
    // Assumes that stream is in binary mode
    // writes delimiters as well (since ints are 4 bytes on hadoop server
    // I chose to write size of int * 2 so that ints are 8 bytes)
    void writeRecord(fstream &indexFile) {

        // Check to see if ints are of the right size (8 bytes)
        cout << "Size of int: " << sizeof(id) << " byte(s)" << endl;
        cout << "Size of int should be: " << sizeof(id) * 2 << " byte(s)" << endl;
        indexFile.write(reinterpret_cast<const char *>(&id), sizeof(id) * 2);
        indexFile << "~";
        indexFile << name;
        indexFile << "~";
        indexFile << bio;
        indexFile << "~";
        indexFile.write(reinterpret_cast<const char *>(&manager_id), sizeof(manager_id) * 2);
        indexFile << "~";

    }
};

// This class is mainly for parsing blocks, so it represents an entire
// block in the index file programmatically, which makes it easy to keep track
// of 3 blocks + page directory memory limit in main memory (logical block rather than physical
// block).
class Block {
private:
    const int PAGE_SIZE = 4096;

public:
    // Logical records in the block
    vector<Record> records;

    // Keep track of block size for easy way to calculate average utilization of block
    // to see if we need to increment n
    int blockSize;

    // Overflow pointer idx value
    int overflowPtrIdx;

    // Number of records in the block
    int numRecords;

    // Physical index of the block (offset index to block location in index file)
    int blockIdx;

    Block() {
        blockSize = 0;
        blockIdx = 0;
    }

    Block(int physIdx) {
        blockSize = 0;
        blockIdx = physIdx;
    }

    // Reads physical block from index file and represents the block logically
    void readBlock(fstream &inputFile) {

        // First get to physical block in index file with seekp()
        inputFile.seekp(blockIdx * PAGE_SIZE);

        // All blocks are initialized with overflow pointer and number of records
        // so we always read these in.
        inputFile.read(reinterpret_cast<char *>(&overflowPtrIdx), sizeof(overflowPtrIdx));
        inputFile.read(reinterpret_cast<char *>(&numRecords), sizeof(numRecords));

        // Now read the number of records in the block
        // FINISH TMR!!!!!!!!!!!!!!!!

    }
};


class LinearHashIndex {

private:
    const int PAGE_SIZE = 4096;

    vector<int> pageDirectory;  // Where pageDirectory[h(id)] gives page index of block
                                // can scan to pages using index*PAGE_SIZE as offset (using seek function)
    int numBlocks; // Now is actual count of blocks including overflow

    // determines the index for page directory (index value of bucket which the last i bits need to match)
    // the page directory then returns an index which is technically in offset into the actual file to read
    // an abstract arbitrary block in index file.
    int numBuckets; // buckets are not the same as blocks (bucket point to block, block hold records) // n
    int i;
    int numRecords; // Records in index
    int nextFreePage; // Next page to write to
    string fName; // Name of output index file

    // Get record from input file (convert .csv row to Record data structure)
    Record getRecord(fstream &recordIn) {

        string line, word;

        // Make vector of strings (fields of record)
        // to pass to constructor of Record struct
        vector<std::string> fields;

        // grab entire line
        if (getline(recordIn, line, '\n'))
        {
            // turn line into a stream
            stringstream s(line);

            // gets everything in stream up to comma
            // and store in respective field in fields vector
            getline(s, word, ',');
            fields.push_back(word);
            getline(s, word, ',');
            fields.push_back(word);
            getline(s, word, ',');
            fields.push_back(word);
            getline(s, word, ',');
            fields.push_back(word);

            return Record(fields);
        }
        else
        {
            // Put error indicator in first field
            fields.push_back("-1");
            fields.push_back("-1");
            fields.push_back("-1");
            fields.push_back("-1");
            return Record(fields);

        }

    }

    // Hash function
    int hash(int id) {
        return (id % (int)pow(2, 16));
    }

    // Function to get last i'th bits of hash value
    int getLastIthBits(int hashVal, int i) {
        return hashVal & ((1 << i) - 1);
    }

    // Initializes a bucket/block
    // Block format:
    // overflow pointer (integer offset index to overflow block in index file), number of records, and then
    // records with delimiters
    // For creating entirely new buckets, not for overflow blocks in existing bucket
    void initBucket(fstream &indexFile) {

        // Bucket index is based on number of blocks starting at 0
        // update numBuckets when creating buckets
        pageDirectory.push_back(numBuckets++);
        numBlocks++;

        int overflowIdx = -1;
        int defaultNumRecords = 0;

        // Pre-write overflow pointer (4 bytes) and number of records (4 byte) to the blocks at the buckets
        // Default overflow pointer value is -1 (since we don't overflow yet), and number of records is 0 for empty block
        indexFile.write(reinterpret_cast<const char *>(&overflowIdx), sizeof(overflowIdx));
        indexFile.write(reinterpret_cast<const char *>(&defaultNumRecords), sizeof(defaultNumRecords));

    }

    // Insert new record into index
    void insertRecord(Record record, fstream &indexFile) {

        // No records written to index yet
        if (numRecords == 0) {
            // Initialize index with first blocks (start with 2)
            // buckets
            for (int i = 0; i < 2; i++) {
                
            }

            // 1 bit needed to address 2 buckets/blocks
            i = 1;
            
        }

        // Add record to the index in the correct block, creating overflow block if necessary
        // hash to get index hash and then take last i'th bits to get bucket index
        int bucketIdx = getLastIthBits(hash(record.id), i);

        // Debug print last i'th bits
        cout << "Bucket index: " << bucketIdx << endl;
        cout << "Last " << i << " bit(s): " << bitset<16>(bucketIdx) << endl;

        // If value of last i'th bits >= n, then flip MSB from 1 to 0
        // If we need to get non-flipped bucketIdx then just hash the entry again?
        if (bucketIdx >= numBuckets) {
            bucketIdx &= ~(1 << (i-1));
        }
        
        // then insert in index file at the bucket index (pgdir[bucket_idx] gives actual offset idx for index file)
        int pgIdx = pageDirectory[bucketIdx];

        // Use seek to get to index*PAGE_SIZE offset
        indexFile.seekp(pgIdx * PAGE_SIZE);

        // NOTE: PUT CODE TO ITERATE TO EMPTY SPOT IN BLOCK LATER
        // RIGHT NOW IT JUST WRITES IT AT START OF BLOCK
        // Write at that block spot in index file, delimit variable record with '~'
        // indexFile << record.id << "~" << record.name << "~" << record.bio << "~" << record.manager_id << "~";
        record.writeRecord(indexFile);
        numRecords++;

        // Take neccessary steps if capacity is reached


    }

public:
    LinearHashIndex(string indexFileName) {
        numBlocks = 0;
        i = 0;
        numRecords = 0;
        numBuckets = 0;
        fName = indexFileName;
    }

    // Read csv file and add records to the index
    void createFromFile(string csvFName) {
        
        // Open filestream to index file and another to .csv file
        fstream indexFile(fName, ios::out | ios::trunc | ios::binary);
        fstream inputFile(csvFName, ios::in);

        if (inputFile.is_open())
            cout << "Employee.csv opened" << endl;

        /* Loop through input file and get all records to add
         * with insertRecord function
         */

        // Flag for checking when we are done reading input file
        bool recordsRemaining = true;

        while (recordsRemaining) {

            Record singleRec = getRecord(inputFile);

            // Debug
            singleRec.print();

            // Check if there are no more records to read
            // and end loop, otherwise insert into index
            if (singleRec.id == -1) {

                cout << "All records read!" << endl;
                recordsRemaining = false;

            }
            else {

                insertRecord(singleRec, indexFile);

            }

            

        }

        // Close filestreams
        indexFile.close();
        inputFile.close();

    }

    // Given an ID, find the relevant record and print it
    Record findRecordById(int id) {
        
    }
};
