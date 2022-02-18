#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <bitset>
#include <fstream>
#include <cmath>
#include <cstdio>
#include <cstdint>
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
        // We have 4 delimiters so 4 bytes extra as well
        return 8 + 8 + bio.length() + name.length() + 4;

    }

    // Takes output stream and writes all member variables (not member functions) to it.
    // Assumes that stream is in binary mode
    // writes delimiters as well (since ints are 4 bytes on hadoop server
    // I chose to write size of int * 2 so that ints are 8 bytes)
    void writeRecord(fstream &indexFile) {

        int64_t paddedId = (int64_t)id;
        int64_t paddedManagerId = (int64_t)manager_id;

        // cout << "Size of padded ID: " << sizeof(paddedId) << " byte(s)" << endl;
        // cout << "Size of padded manager ID: " << sizeof(paddedManagerId) << " byte(s)" << endl;

        // Cast int members to int64_t to write 8 bytes to index file
        indexFile.write(reinterpret_cast<const char *>(&paddedId), sizeof(paddedId));
        indexFile << "~";
        indexFile << name;
        indexFile << "~";
        indexFile << bio;
        indexFile << "~";
        indexFile.write(reinterpret_cast<const char *>(&paddedManagerId), sizeof(paddedManagerId));
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

    // Reads record from index file
    // Assumes you are at correct position in block to start reading record
    // and that the format matches what was written in writeRecord()
    void readRecord(fstream &inputFile) {
        
        // Get 8 byte ints to temporarily store 8 byte ints in file
        // before converting back to 4 byte ints used in the Record class
        int64_t paddedId;
        int64_t paddedManagerId;
        string name, bio;

        inputFile.read(reinterpret_cast<char *>(&paddedId), sizeof(paddedId));
        // Ignore delimeter right after binary int
        inputFile.ignore(1, '~');
        getline(inputFile, name, '~');
        getline(inputFile, bio, '~');
        inputFile.read(reinterpret_cast<char *>(&paddedManagerId), sizeof(paddedManagerId));
        // Ignore delimeter right after binary int
        inputFile.ignore(1, '~');

        cout << paddedId << endl;
        cout << name << endl;
        cout << bio << endl;
        cout << paddedManagerId << endl;

        int regularId = (int)paddedId;
        int regularManagerId = (int)paddedManagerId;
        vector<string> fields{to_string(regularId), name, bio, to_string(regularManagerId)};

        records.push_back(Record(fields));

    }

    // Reads physical block from index file and represents the block logically
    void readBlock(fstream &inputFile) {

        // First get to physical block in index file with seekp()
        inputFile.seekg(blockIdx * PAGE_SIZE);

        // All blocks are initialized with overflow pointer and number of records
        // so we always read these in.
        inputFile.read(reinterpret_cast<char *>(&overflowPtrIdx), sizeof(overflowPtrIdx));
        inputFile.read(reinterpret_cast<char *>(&numRecords), sizeof(numRecords));

        cout << "\n\n" << "-------------------------------------------" << endl;

        cout << "Overflow idx: " << overflowPtrIdx << endl;
        cout << "# of records: " << numRecords << endl;

        // Add to block size 8 since overflow idx and number of records are 4 bytes each
        blockSize += 8;

        // Now read the number of records in the block
        for (int i = 0; i < numRecords; i++) {

            readRecord(inputFile);

            // Update block size as records are being pushed back into vector
            // read current index as a record was just pushed back into vector
            // at that index
            blockSize += records[i].calcSize();
            cout << "*** Current Block's Size: " << blockSize << endl;

        }

        cout << "-------------------------------------------" << "\n\n" << endl;

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

        int overflowIdx = -1;
        int defaultNumRecords = 0;

        // Pre-write overflow pointer (4 bytes) and number of records (4 byte) to the blocks at the buckets
        // Default overflow pointer value is -1 (since we don't overflow yet), and number of records is 0 for empty block
        indexFile.seekp(numBuckets * PAGE_SIZE);

        indexFile.write(reinterpret_cast<const char *>(&overflowIdx), sizeof(overflowIdx));
        indexFile.write(reinterpret_cast<const char *>(&defaultNumRecords), sizeof(defaultNumRecords));

        // Bucket index is based on number of blocks starting at 0
        // update numBuckets when creating buckets
        pageDirectory.push_back(numBuckets++);
        numBlocks++;
        
    }

    // Create overflow block (we DON'T ADD TO PAGE DIRECTORY SINCE NOT NEW BUCKET!) on existing bucket
    // We also need to overwrite the overflow index pointer of the block that points
    // to this overflow block
    // Returns physical offset index of overflow block
    int initOverflowBlock(int parentBlockIdx, fstream &indexFile) {

        int overflowIdx = -1;
        int defaultNumRecords = 0;

        // Since index of current overflow block
        int currIdx = numBlocks;
        numBlocks++;

        // Write boilerplate block info (overflow index to NEXT OVERFLOW BLOCK and # of records)
        // which are initial values since this is fresh overflow block
        indexFile.seekp(currIdx * PAGE_SIZE);

        indexFile.write(reinterpret_cast<const char *>(&overflowIdx), sizeof(overflowIdx));
        indexFile.write(reinterpret_cast<const char *>(&defaultNumRecords), sizeof(defaultNumRecords));

        // Rewrite parent block's overflow index to point to this current index to link parent to current
        // Overflow index is first 4 bytes so immediately just overwrite the first 4 bytes with current index
        indexFile.seekp(parentBlockIdx * PAGE_SIZE);
        indexFile.write(reinterpret_cast<const char *>(&currIdx), sizeof(currIdx));

        // Return index of current overflow block
        return currIdx;
        
    }


    // Insert new record into index
    void insertRecord(Record record, fstream &indexFile) {

        // No records written to index yet
        if (numRecords == 0) {
            // Initialize index with first blocks (start with 2)
            // buckets
            for (int i = 0; i < 2; i++) {
                initBucket(indexFile);
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
        // Deals with virtual/ghost buckets
        if (bucketIdx >= numBuckets) {
            bucketIdx &= ~(1 << (i-1));
        }
        
        // then insert in index file at the bucket index (pgdir[bucket_idx] gives actual offset idx for index file)
        int pgIdx = pageDirectory[bucketIdx];

        // Use seek to get to index*PAGE_SIZE offset
        indexFile.seekp(pgIdx * PAGE_SIZE);

        // Write at that block spot in index file, delimit variable record with '~'
        // indexFile << record.id << "~" << record.name << "~" << record.bio << "~" << record.manager_id << "~";
        // FIND EMPTY SPOT WITHIN BLOCK IF POSSIBLE OTHERWISE OVERFLOW
        // READ BLOCK/PARSE BLOCK, THEN USE CALCSIZE FUNCTION TO CALCULATE TAKEN SPACE AND THEN THE NEXT
        // FREE BYTE SPOT TO WRITE RECORD TO (MOVE WITH SEEKP TO THAT SPOT) THEN WRITE RECORD

        // We may or may not need to navigate through multiple blocks before writing record
        // Especially if there are multiple overflow blocks
        // This flag is to mark when we've written the record, otherwise we continue iterating
        // through overflow blocks if they exist until we find a spot to put the record (if the
        // initial block is full that is)
        bool hasWrittenRecord = false;

        while (!hasWrittenRecord) {

            // Read/parse current block that we just indexed to
            Block currBlock(pgIdx);
            currBlock.readBlock(indexFile);

            // Check if current record fits inside current block,
            // if not then see if there is overflow and check overflow for space
            // if there isn't, then create overflow and write record there
            if (currBlock.blockSize + record.calcSize() <= PAGE_SIZE) {
                
                cout << "== New size of block after record added: " << currBlock.blockSize + record.calcSize() << endl;

                // Record fits completely within block, so write at empty spot
                // (Move file pointer up to current blockSize and write record there)
                indexFile.seekp((pgIdx * PAGE_SIZE) + currBlock.blockSize);
                record.writeRecord(indexFile);

                // Update current block's # of records field
                int newNumRecords = currBlock.numRecords + 1;
                // Move to start of block and jump over 8 bytes of overflow index to write at # of records field
                indexFile.seekp((pgIdx * PAGE_SIZE) + sizeof(int));
                indexFile.write(reinterpret_cast<const char *>(&newNumRecords), sizeof(newNumRecords));

                // Set flag to true because we wrote record
                hasWrittenRecord = true;

            }
            else if (currBlock.overflowPtrIdx != -1) {
                
                // This means that current block is full, but there exists a linked overflow block
                cout << "== No space in current block, moving to existing overflow block to check..." << endl;

                // Set pgIdx to overflow idx of the current block so that next loop iteration the currBlock
                // will be the overflow block and we can continue this iteration logic for writing record
                pgIdx = currBlock.overflowPtrIdx;

            }
            else {

                // Create overflow since no overflow block exists
                cout << "== Since the block is now " << currBlock.blockSize + record.calcSize() << " bytes after adding new record, create overflow block instead." << endl;

                // Initialize overflow block and return its physical offset index
                int overflowIdx = initOverflowBlock(pgIdx, indexFile);

                // Now move to overflow block and write record to 
                indexFile.seekp(overflowIdx * PAGE_SIZE);
                record.writeRecord(indexFile);

                // Update current block's # of records field
                int newNumRecords = currBlock.numRecords + 1;
                // Move to start of block and jump over 8 bytes of overflow index to write at # of records field
                indexFile.seekp((overflowIdx * PAGE_SIZE) + sizeof(int));
                indexFile.write(reinterpret_cast<const char *>(&newNumRecords), sizeof(newNumRecords));

                // Set flag
                hasWrittenRecord = true;

            }

        }

        // Increment # of records
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
        
        // Open filestream to index file (we read and write from index so in and out both set) and another to .csv file
        fstream indexFile(fName, ios::in | ios::out | ios::trunc | ios::binary);
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
            // singleRec.print();

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
