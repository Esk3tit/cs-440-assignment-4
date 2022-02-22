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

        cout << "READ IN: " << paddedId << " " << name << endl;
        // cout << name << endl;
        // cout << bio << endl;
        // cout << paddedManagerId << endl;

        int regularId = (int)paddedId;
        int regularManagerId = (int)paddedManagerId;
        vector<string> fields{to_string(regularId), name, bio, to_string(regularManagerId)};

        records.push_back(Record(fields));

    }

    // Reads physical block from index file and represents the block logically
    void readBlock(fstream &inputFile) {

        // First get to physical block in index file with seekg()
        inputFile.seekg(blockIdx * PAGE_SIZE);

        // All blocks are initialized with overflow pointer and number of records
        // so we always read these in.
        inputFile.read(reinterpret_cast<char *>(&overflowPtrIdx), sizeof(overflowPtrIdx));
        inputFile.read(reinterpret_cast<char *>(&numRecords), sizeof(numRecords));

        cout << "\n" << "v-------------------------------------------v" << endl;
        cout << "[READING BLOCK]" << endl;
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
            cout << "**** Current Block's Size: " << blockSize << endl;

        }

        cout << "^-------------------------------------------^" << "\n" << endl;

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

    // Bookkeeping vars for debugging and statistics
    int numOverflowBlocks;

    // Vars for calculating average number of records per block
    int currentTotalSize;

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
    // Return LOGICAL BUCKET index (not physical offset index!!!!)
    int initBucket(fstream &indexFile) {

        int overflowIdx = -1;
        int defaultNumRecords = 0;

        // Pre-write overflow pointer (4 bytes) and number of records (4 byte) to the blocks at the buckets
        // Default overflow pointer value is -1 (since we don't overflow yet), and number of records is 0 for empty block
        indexFile.seekp(nextFreePage * PAGE_SIZE);

        indexFile.write(reinterpret_cast<const char *>(&overflowIdx), sizeof(overflowIdx));
        indexFile.write(reinterpret_cast<const char *>(&defaultNumRecords), sizeof(defaultNumRecords));

        // Bucket index is based on number of blocks starting at 0
        // update numBuckets when creating buckets
        // We want to have phys index based on # of blocks because we can allocate more blocks outside
        // of this function and so we want the latest free block index for the bucket
        // # of bucket probably < # of blocks most of the time, so if we allocate phys idx by # of buckets
        // we would allocate an already used phys idx most likely
        pageDirectory.push_back(nextFreePage++);
        numBlocks++;
        numBuckets++;

        // Update current total size
        currentTotalSize += sizeof(overflowIdx) + sizeof(defaultNumRecords);

        return pageDirectory.size() - 1;
        
    }

    // Create overflow block (we DON'T ADD TO PAGE DIRECTORY SINCE NOT NEW BUCKET!) on existing bucket
    // We also need to overwrite the overflow index pointer of the block that points
    // to this overflow block
    // Returns physical offset index of overflow block
    int initOverflowBlock(int parentBlockIdx, fstream &indexFile) {

        int overflowIdx = -1;
        int defaultNumRecords = 0;

        // Get index of current overflow block
        int currIdx = nextFreePage++;

        // Write boilerplate block info (overflow index to NEXT OVERFLOW BLOCK and # of records)
        // which are initial values since this is fresh overflow block
        indexFile.seekp(currIdx * PAGE_SIZE);

        indexFile.write(reinterpret_cast<const char *>(&overflowIdx), sizeof(overflowIdx));
        indexFile.write(reinterpret_cast<const char *>(&defaultNumRecords), sizeof(defaultNumRecords));

        // Rewrite parent block's overflow index to point to this current index to link parent to current
        // Overflow index is first 4 bytes so immediately just overwrite the first 4 bytes with current index
        indexFile.seekp(parentBlockIdx * PAGE_SIZE);
        indexFile.write(reinterpret_cast<const char *>(&currIdx), sizeof(currIdx));

        // Update current total size (8 new bytes only, since 4 bytes of the parent are already counted)
        currentTotalSize += sizeof(overflowIdx) + sizeof(defaultNumRecords);

        // Update number of overflow blocks and blocks
        numBlocks++;
        numOverflowBlocks++;

        // Return index of current overflow block
        return currIdx;
        
    }

    // Initialize empty block, return physical offset index to new block
    int initEmptyBlock(fstream &indexFile) {

        int overflowIdx = -1;
        int defaultNumRecords = 0;

        // Get index for current block
        int currIdx = nextFreePage++;

        indexFile.seekp(currIdx * PAGE_SIZE);

        indexFile.write(reinterpret_cast<const char *>(&overflowIdx), sizeof(overflowIdx));
        indexFile.write(reinterpret_cast<const char *>(&defaultNumRecords), sizeof(defaultNumRecords));

        // Update current total size (8 new bytes only, since 4 bytes of the parent are already counted)
        currentTotalSize += sizeof(overflowIdx) + sizeof(defaultNumRecords);

        // Update number of blocks
        numBlocks++;

        return currIdx;

    }

    // Modular function to write record to physical file
    // Reducing redundancy
    // NOTE: MEETS THREE BLOCKS REQUIREMENT, WE LOOK AT ONE BLOCK AT A TIME TO SEE IF
    // WE CAN WRITE RECORD THERE, OTHERWISE WE CHECK OR CREATE OVERFLOW BLOCKS
    void writeRecordToIndexFile(Record record, int baseBlockPgIdx, fstream &indexFile) {

        bool hasWrittenRecord = false;

        while (!hasWrittenRecord) {

            // Read/parse current block that we just indexed to
            Block currBlock(baseBlockPgIdx);
            currBlock.readBlock(indexFile);

            // Check if current record fits inside current block,
            // if not then see if there is overflow and check overflow for space
            // if there isn't, then create overflow and write record there
            if (currBlock.blockSize + record.calcSize() <= PAGE_SIZE) {
                
                cout << "== New block size after record added: " << currBlock.blockSize + record.calcSize() << "\n" << endl;

                // Record fits completely within block, so write at empty spot
                // (Move file pointer up to current blockSize and write record there)
                indexFile.seekp((baseBlockPgIdx * PAGE_SIZE) + currBlock.blockSize);
                record.writeRecord(indexFile);

                // Update current block's # of records field
                int newNumRecords = currBlock.numRecords + 1;
                // Move to start of block and jump over 4 bytes of overflow index to write at # of records field
                indexFile.seekp((baseBlockPgIdx * PAGE_SIZE) + sizeof(int));
                indexFile.write(reinterpret_cast<const char *>(&newNumRecords), sizeof(newNumRecords));

                // Update current total size
                currentTotalSize += record.calcSize();

                // Set flag to true because we wrote record
                hasWrittenRecord = true;

            }
            else if (currBlock.overflowPtrIdx != -1) {
                
                // This means that current block is full, but there exists a linked overflow block
                cout << "== Block full w/ overflow block, moving to existing overflow block w/ physical index " << currBlock.overflowPtrIdx << "\n" << endl;

                // Set baseBlockPgIdx to overflow idx of the current block so that next loop iteration the currBlock
                // will be the overflow block and we can continue this iteration logic for writing record
                baseBlockPgIdx = currBlock.overflowPtrIdx;

            }
            else {

                // Create overflow since no overflow block exists
                cout << "== Block full and no overflow block. Creating overflow block..." << "\n" << endl;

                // Initialize overflow block and return its physical offset index
                int overflowIdx = initOverflowBlock(baseBlockPgIdx, indexFile);

                // Now move to overflow block and write record after the overflow index and # of records in that block
                indexFile.seekp((overflowIdx * PAGE_SIZE) + sizeof(int) + sizeof(int));
                record.writeRecord(indexFile);

                // Update current block's # of records field (only 1 record after writing current record into overflow block)
                int newNumRecords = 1;
                // Move to start of block and jump over 4 bytes of overflow index to write at # of records field
                indexFile.seekp((overflowIdx * PAGE_SIZE) + sizeof(int));
                indexFile.write(reinterpret_cast<const char *>(&newNumRecords), sizeof(newNumRecords));

                // Update current total size
                currentTotalSize += record.calcSize();

                // Set flag
                hasWrittenRecord = true;

            }

        }

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
        

        // If value of last i'th bits >= n, then set MSB from 1 to 0
        // Deals with virtual/ghost buckets
        if (bucketIdx >= numBuckets) {
            cout << "Set bucket index MSB to 0, # of buckets is: " << numBuckets << endl;
            bucketIdx &= ~(1 << (i-1));
        }
        
        // then insert in index file at the bucket index (pgdir[bucket_idx] gives actual offset idx for index file)
        int pgIdx = pageDirectory[bucketIdx];
        cout << "Physical offset index of bucket index " << bucketIdx << ": " << pgIdx << endl;

        // Write at that block spot in index file, delimit variable record with '~'
        // FIND EMPTY SPOT WITHIN BLOCK IF POSSIBLE OTHERWISE OVERFLOW
        // READ BLOCK/PARSE BLOCK, THEN USE CALCSIZE FUNCTION TO CALCULATE TAKEN SPACE AND THEN THE NEXT
        // FREE BYTE SPOT TO WRITE RECORD TO (MOVE WITH SEEKP TO THAT SPOT) THEN WRITE RECORD

        // We may or may not need to navigate through multiple blocks before writing record
        // Especially if there are multiple overflow blocks
        // This flag is to mark when we've written the record, otherwise we continue iterating
        // through overflow blocks if they exist until we find a spot to put the record (if the
        // initial block is full that is)

        writeRecordToIndexFile(record, pgIdx, indexFile);

        // Increment # of records
        numRecords++;

        // Take neccessary steps if capacity is reached
        // Calculate average bytes capacity per bucket (not block) is > 70%
        double avgCapacityPerBucket = (double)currentTotalSize / numBuckets;

        // 70% of block capacity is 70% of 4096 or .7 * 4096
        if (avgCapacityPerBucket > 0.7 * PAGE_SIZE) {

            // Add a new bucket
            // Note: numBuckets is incremented at this point
            // Find bucket to rehash and move to new bucket
            int newBucketIdx = initBucket(indexFile);

            // Now calculate the number of binary digits needed to address the new bucket
            // ex. For third bucket with index 2, we need 2 binary digits to address 3 buckets
            int digitsToAddrNewBucket = (int)ceil(log2(numBuckets));

            /* Rehash some search keys into this new bucket
             * calculate the index of the real bucket that was used to hold
             * records when the new bucket was still a ghost bucket
             * ex. When adding third bucket, before third bucket with index 2 (binary: 10)
             * was added, keys that were hashed to index 2 (binary: 10) had their MSB
             * set to 0 to get a real bucket whose index was < than # of buckets, which
             * in this case was bucket 0. So we rehash everything in bucket 0 to see if they need
             * to be moved to the new bucket which now exists and is no longer a ghost bucket
             */
            int realBucketToMoveRecordsFromIdx = newBucketIdx;
            realBucketToMoveRecordsFromIdx &= ~(1 << (digitsToAddrNewBucket - 1));

            // Debug prints
            cout << "** Number of buckets: " << numBuckets << endl;
            cout << "** New bucket index (numBuckets - 1): " << newBucketIdx << endl;
            cout << "** New bucket index binary (numBuckets - 1): " << bitset<16>(newBucketIdx) << endl;
            cout << "** Real bucket index to rehash to new bucket (binary): " << bitset<16>(realBucketToMoveRecordsFromIdx) << endl;

            // Ghost bucket is now a real new bucket, move ghost search keys to this new real bucket
            int realBucketToMoveRecordsFromPgIdx = pageDirectory[realBucketToMoveRecordsFromIdx];
            
            // Iterate through entire bucket (base block + overflow blocks) and any ghost keys to new real bucket
            // NOTE: TO AVOID HAVING TO MANUALLY DELETE RECORDS FROM A BLOCK (WHICH IS EITHER TEDIOUS OR HEAVY ON I/O)
            // WE INSTEAD CHOOSE TO ALLOCATE TWO NEW BLOCKS, ONE IS THE FOR "NEW" OLD BUCKET AND ONE IS NEW REAL BUCKET, WRITE THE RECORDS
            // TO THE RESPECTIVE BUCKET OF THE TWO AND DELETE THE TRUE OLD BUCKET THAT WE WOULD ACTUALLY REMOVE RECORDS FROM

            // We now allocate second block for "new" old block which is basically the old block without the ghost keys after this process
            int newOldBucketPgIdx = initEmptyBlock(indexFile);

            while (realBucketToMoveRecordsFromPgIdx != -1) {

                // Read block at old bucket with ghost keys
                Block oldBlock(realBucketToMoveRecordsFromPgIdx);
                oldBlock.readBlock(indexFile);

                // Parsed entire block in Block object, so we can cleanup
                // block by blanking out entire block with asterisks
                cout << "Cleaning up block at physical index " << oldBlock.blockIdx << endl;
                indexFile.seekp(oldBlock.blockIdx * PAGE_SIZE);
                indexFile << string(PAGE_SIZE, '*');

                // Decrement number of blocks and numOverflowBlock (but increment # of overflow block again after since first
                // block is not overflow)
                numBlocks--;
                numOverflowBlocks--;

                // Update size and number of records, we "zero" out entire block, but only the block's size
                // of data is removed
                currentTotalSize -= oldBlock.blockSize;
                numRecords -= oldBlock.numRecords;

                // Check each record in old block and determine which record goes to which block
                // NOTE: MEETS THREE BLOCKS REQUIREMENT, WE LOOK AT ONE BLOCK AT A TIME IN EACH CONDITIONAL BRANCH
                // TO SEE IF WE CAN INSERT A RECORD IN THAT BLOCK FOR THE BUCKET, OTHERWISE WE CHECK OR CREATE OVERFLOW BLOCKS
                // (EVEN IF YOU CONSIDER THIS TWO BLOCKS IN MEMORY AT THE SAME TIME BECAUSE OF THE TWO BUCKET "BRANCHES" IT STILL
                // MEETS REQUIREMENTS OF BEING UNDER THREE SINCE IN EACH BRANCH WE ONLY LOOK AT ONE BLOCK AT A TIME MEANING THAT
                // ONLY TWO BLOCKS AT MOST ARE EVER LOADED IN MAIN MEMORY AT ONCE)
                for (int i = 0; i < oldBlock.numRecords; i++) {

                    // Consider last digitsToAddrNewBucket number of bits for each record
                    // If it matches the index of the new bucket then move to new bucket's block
                    // otherwise write to new old bucket block.
                    // Ex. For third new bucket at index 2 (binary: 10), we look at index 0 bucket for rehash and moving
                    // ghost keys; we now need to consider last 2 binary digits for each hashed id to see if it stays in current old
                    // bucket (last binary digits are 00) or gets moved to new bucket at index 2 (last binary digits are 10).
                    if (getLastIthBits(hash(oldBlock.records[i].id), digitsToAddrNewBucket) == newBucketIdx) {

                        // Put record in new bucket
                        int newBucketBlockPgIdx = pageDirectory[newBucketIdx];
                        writeRecordToIndexFile(oldBlock.records[i], newBucketBlockPgIdx, indexFile);

                    }
                    else {

                        // Put record in "new" old bucket that will have all ghost keys removed
                        int tempNewOldBlockPgIdx = newOldBucketPgIdx;
                        writeRecordToIndexFile(oldBlock.records[i], tempNewOldBlockPgIdx, indexFile);

                    }

                    numRecords++;

                }

                // Move up realBucketToMoveRecordsFromPgIdx iterator to its overflow index to continue outer while loop to rehash
                // all records in the bucket including overflow block records
                realBucketToMoveRecordsFromPgIdx = oldBlock.overflowPtrIdx;

            }
            
            // Increment overflow block again because first block in bucket is not overflow
            numOverflowBlocks++;

            // Re-hook up the pageDirectory to point to these new blocks for the old bucket
            pageDirectory[realBucketToMoveRecordsFromIdx] = newOldBucketPgIdx;

            // Update i as well
            i = digitsToAddrNewBucket;

        }

    }

public:
    LinearHashIndex(string indexFileName) {
        numBlocks = 0;
        i = 0;
        numRecords = 0;
        numBuckets = 0;
        fName = indexFileName;
        numOverflowBlocks = 0;
        currentTotalSize = 0;
        nextFreePage = 0;
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

        // Print out stats for validation of results
        cout << "\n\n" << "----------------------------------------------------------------------------" << endl;
        cout << "[FINAL STATS]" << endl;
        cout << "# of buckets: " << numBuckets << endl;
        cout << "# of blocks: " << numBlocks << endl;
        cout << "# of overflow blocks: " << numOverflowBlocks << endl;
        cout << "# of records: " << numRecords << endl;
        cout << "Average capacity per bucket (decimal percentage): " << (double)currentTotalSize / (numBuckets * PAGE_SIZE) << endl;
        cout << "----------------------------------------------------------------------------" << endl;

        // Close filestreams
        indexFile.close();
        inputFile.close();

    }

    // Given an ID, find the relevant record and print it
    Record findRecordById(int id) {

        // Open file
        fstream indexFile(fName, ios::in);
        
        // Calculate bucket index and get last i'th bits
        // since we insert before we search, we can assume that the member variable i
        // is already the right value to address all buckets and so we can reuse it here
        // for calculating the bucket index of the target id
        int bucketIdx = getLastIthBits(hash(id), i);

        // Check if the record is going to be in real or fake/ghost bucket
        // If value of last i'th bits >= n, then set MSB from 1 to 0
        // Deals with virtual/ghost buckets
        if (bucketIdx >= numBuckets) {
            cout << "[SEARCH] Set bucket index MSB to 0, # of buckets is: " << numBuckets << endl;
            bucketIdx &= ~(1 << (i-1));
        }

        // Iterate through block by block of the bucket (base + overflow blocks)
        // until record with id is found

        // Get page index
        int pgIdx = pageDirectory[bucketIdx];

        while (pgIdx != -1) {
            
            // Read block
            // NOTE: MEETS 3 BLOCKS IN MAIN MEMORY REQUIREMENT
            // WE READ ONE BLOCK AT A TIME AND THEN MOVE TO NEXT BLOCK
            Block currBlock(pgIdx);
            currBlock.readBlock(indexFile);

            // Check if record with target ID in block
            for (int i = 0; i < currBlock.numRecords; i++) {

                // Return record if found
                if(currBlock.records[i].id == id)
                    return currBlock.records[i];

            }

            // Move up pgIdx to overflow block for next iteration
            pgIdx = currBlock.overflowPtrIdx;

        }

        indexFile.close();

    }
};
