#include "src/include/rbfm.h"
#include <iostream>
#include <cmath>
#include <cstring>
namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        PagedFileManager& pagedFileManager = PagedFileManager::instance();
        RC return_val = pagedFileManager.createFile(fileName);

        return return_val;
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        PagedFileManager& pagedFileManager = PagedFileManager::instance();
        RC return_val = pagedFileManager.destroyFile(fileName);
        return return_val;
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        PagedFileManager& pagedFileManager = PagedFileManager::instance();
        RC return_val = pagedFileManager.openFile(fileName, fileHandle);
        return return_val;
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        PagedFileManager& pagedFileManager = PagedFileManager::instance();
        RC return_val = pagedFileManager.closeFile(fileHandle);
        return return_val;
    }


    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        int recordSize = 0;
        //It will store the size of the record in the recordSize variable
        void * record = encoder(recordDescriptor, data, recordSize);
        int pageNums = fileHandle.getNumberOfPages();
        //Page num in which data finally got inserted
        /*
         * If pageNums is zero, then this is the first record and
         * we need to append a page*/

        if(pageNums == 0){
            int newPageIndex = insertDataNewPage(fileHandle, recordSize, record);
            rid.pageNum = newPageIndex;
            rid.slotNum = 1;
        }else{
            int freePageIndex = findFreePageIndex(fileHandle, recordSize);
            //No suitable  free space found, so append a new page
            if(freePageIndex == -1){
                int newPageIndex = insertDataNewPage(fileHandle, recordSize, record);
                rid.pageNum = newPageIndex;
                rid.slotNum = 1;
            } else {
                //insert data in the found page
                rid.slotNum = insertDataByPageIndex(fileHandle, freePageIndex, record, recordSize);
                rid.pageNum = freePageIndex;
            }
        }
        free(record);
        return 0;
    }

    RC RecordBasedFileManager::insertMiscData(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, void *record, int recordSize, RID &rid) {

        int pageNums = fileHandle.getNumberOfPages();
        //Page num in which data finally got inserted
        /*
         * If pageNums is zero, then this is the first record and
         * we need to append a page*/

        if(pageNums == 0){
            int newPageIndex = insertDataNewPage(fileHandle, recordSize, record);
            rid.pageNum = newPageIndex;
            rid.slotNum = 1;
        }else{
            int freePageIndex = findFreePageIndex(fileHandle, recordSize);
            //No suitable  free space found, so append a new page
            if(freePageIndex == -1){
                int newPageIndex = insertDataNewPage(fileHandle, recordSize, record);
                rid.pageNum = newPageIndex;
                rid.slotNum = 1;
            } else {
                //insert data in the found page
                rid.slotNum = insertDataByPageIndex(fileHandle, freePageIndex, record, recordSize);
                rid.pageNum = freePageIndex;
            }
        }
        free(record);
        return 0;
    }

    /*
     * Insert data in the hole
     * right shift all other data
     * */
    RC RecordBasedFileManager::insert_data_in_hole(char* page, void* record, int recordSize, int holeNum, int totalSlots){
        unsigned insertAddressForRecord;
        //This is a special case as there is no data on the right from which
        //to get the insertion address. So, the insertion address in this case is zero
        // if totalSlots is 1. Else, we get it from the record that resides on the left
        // (i.e. immediate right slot)
        //If totalSlots is 1, this is the hole itself
        if(totalSlots == 1){
            insertAddressForRecord = 0;
        }
        else if(holeNum == totalSlots){
            //So, in this case totalSlots > 1
            int right_slot_data_address = *(int *)(page + getStartAddressOffset(holeNum-1));
            int right_slot_data_len = *(int *)(page + getLenAddressOffset(holeNum-1));
            insertAddressForRecord = right_slot_data_address + right_slot_data_len;
        }
        else{
            //Hole resides in between the slots. So, get the address of the
            //record that will become immediate right to it (i.e. immediate left slot)
            int left_slot_data_address = *(int *)(page + getStartAddressOffset(holeNum+1));
            insertAddressForRecord = left_slot_data_address;
        }
        //Shift other records to right
        shiftRecordsRight(page, totalSlots, holeNum+1, recordSize);
        //Now that other data are shifted, insert this record
        //Also update the address and length value in the slot of this record
        memcpy(page + insertAddressForRecord, record, recordSize);
        //Update address and length field in slot
        memcpy(page+ getStartAddressOffset(holeNum), &insertAddressForRecord, sizeof (unsigned ));
        memcpy(page+ getLenAddressOffset(holeNum), &recordSize, sizeof (unsigned ));
        return 0;
    }

    //Returns slotNum where record was inserted
    int RecordBasedFileManager::insertDataByPageIndex(FileHandle &fileHandle, int pageIndex, void* record, int recordSize){

        char* page =(char *) malloc(PAGE_SIZE);
        memset(page, 0, PAGE_SIZE);
        fileHandle.readPage(pageIndex, page);
        int start_address_of_slotNum = PAGE_SIZE - 2*sizeof(unsigned);
        //Get Number of slots
        unsigned numOfSlots = *(unsigned *)(page + start_address_of_slotNum);
//        memcpy(&numOfSlots, page + start_address_of_slotNum, sizeof(unsigned ));

        //check if free slot exists
        int holeNum = free_slot_num(page, numOfSlots);
        int insertion_index;
        //Get FreeSpace details
        int start_address_of_freeSpace = PAGE_SIZE - sizeof(unsigned);
        int freeSpace = 0;
        memcpy(&freeSpace, page + start_address_of_freeSpace, sizeof(unsigned));

        //Insert data in free slot, if found a free slot
        if(holeNum != -1){
            insert_data_in_hole(page, record, recordSize, holeNum, numOfSlots);
            insertion_index = holeNum;
            /*Update free space value*
             * No new slot was used*/
            freeSpace = freeSpace - std::max(recordSize,6);
        }else{
            //from numOfSlots, we need to get the start address and length of the last record
            unsigned totalLengthOfMetadata = (2 + 2 * numOfSlots) * sizeof(unsigned); // 1 for F, 1 for N, 2 for each record
            unsigned startAddressOfLastRecord, lengthOfLastRecord;
            memcpy(&startAddressOfLastRecord, page + PAGE_SIZE - totalLengthOfMetadata, sizeof(unsigned));
            memcpy(&lengthOfLastRecord, page + PAGE_SIZE - totalLengthOfMetadata + 4, sizeof(unsigned));
//        unsigned startAddressOfLastRecord = ((unsigned *)(page + PAGE_SIZE - totalLengthOfMetadata))[0];
//        unsigned lengthOfLastRecord = ((unsigned *)(page + PAGE_SIZE - totalLengthOfMetadata))[1];

            unsigned insertAddressForRecord = startAddressOfLastRecord + lengthOfLastRecord;
            memcpy(page + insertAddressForRecord, record, recordSize);
            /*Add slot info*/
//        ((unsigned *)(page + PAGE_SIZE - totalLengthOfMetadata))[-1] = recordSize;
//        ((unsigned *)(page + PAGE_SIZE - totalLengthOfMetadata))[-2] = insertAddressForRecord;
//        numOfSlots++;
//        *(unsigned *)(page + start_address_of_slotNum) = numOfSlots;
            //Add length info
            memcpy(page + PAGE_SIZE - totalLengthOfMetadata - sizeof(unsigned), &recordSize, sizeof(unsigned));
            //Add startAddress
            memcpy(page + PAGE_SIZE - totalLengthOfMetadata - 2*sizeof(unsigned), &insertAddressForRecord, sizeof(unsigned ));
            //Update NumSlots
            numOfSlots++;
            memcpy(page + start_address_of_slotNum, &numOfSlots, sizeof(unsigned) );
            insertion_index = numOfSlots;

            /*Update free space value*/
            freeSpace = freeSpace - 2*sizeof(unsigned) - std::max(recordSize,6);
        }
        /*Update free space*/
        memcpy(page + start_address_of_freeSpace, &freeSpace, sizeof(unsigned));
        fileHandle.writePage(pageIndex, page);
        free(page);
        return insertion_index;
    }
    /*
     * Scan if any slot is free. It will be checked by checking the length field.
     * The length field for a free slot is -1
     * If yes, return the slot number.
     * else, return -1
     * */
    int RecordBasedFileManager::free_slot_num(char* page, int totalSlots){
        for(int i = 1; i <= totalSlots; i++){
            int len_offset = getLenAddressOffset(i);
            int curr_record_len = 0;
            memcpy(&curr_record_len, page + len_offset, sizeof (unsigned ));
            if(curr_record_len == -1) return i;
        }
        return -1;
    }
    /*
     * This will be used in both InsertRecord and upDateRecord (if record is updated to longer length)
     * Starting from the last Slot number to startSlot number, shift all records to right by shiftBy length
     * Update their offset.
     * If startSlot is more than totalSlots, then just return as there is nothing to shift
     * Note: this function does not update the free space after shifting
     * */
    RC RecordBasedFileManager::shiftRecordsRight(char* page, int totalSlots, int startSlot, int shiftBy){
        if(startSlot > totalSlots) return 0;
        //Start by shifting rightmost element first
        for(int i = totalSlots; i >= startSlot; i--){
            int curr_record_start_addr = getStartAddressOffset(i);
            int curr_record_len_addr = getLenAddressOffset(i);
            int curr_record_len = 0;
            int curr_record_data_addr = 0;
            memcpy(&curr_record_data_addr, page + curr_record_start_addr, sizeof (unsigned ));
            //If this is a hole, don't do anything
            if(curr_record_data_addr == -1){
                continue;
            }
            int new_addr_offset = curr_record_data_addr + shiftBy;
            memcpy(&curr_record_len, page + curr_record_len_addr, sizeof (unsigned ));
            //shift the data to right
            memmove(page + new_addr_offset, page + curr_record_data_addr, curr_record_len);
            //Update the data address in the slot
            memcpy(page + curr_record_start_addr, &new_addr_offset, sizeof (unsigned ));
        }
        return 0;
    }
    /*
     * This will be used in both deleteRecord and upDateRecord (if record is updated to shorter length)
     * Starting from the startSlot number to last Slot number, shift all records to left by shiftBy length.
     * Update their offset.
     * If startSlot is more than totalSlots, then just return as there is nothing to shift
     * Note: this function does not update the free space after shifting
     * */
    RC RecordBasedFileManager::shiftRecordsLeft(char* page, int totalSlots, int startSlot, int shiftBy){
        if(startSlot > totalSlots) return 0;
        //start shifting the leftmost element first
        for(int i = startSlot; i <= totalSlots; i++){
            int curr_record_start_addr = getStartAddressOffset(i);
            int curr_record_len_addr = getLenAddressOffset(i);
            int curr_record_len = 0;
            int curr_record_data_addr = 0;
            memcpy(&curr_record_data_addr, page + curr_record_start_addr, sizeof (unsigned ));
            //If this is a hole, don't do anything
            if(curr_record_data_addr == -1){
                continue;
            }
            int new_addr_offset = curr_record_data_addr - shiftBy;
            memcpy(&curr_record_len, page + curr_record_len_addr, sizeof (unsigned ));
            //shift the data to left
            memmove(page + new_addr_offset, page + curr_record_data_addr, curr_record_len);
            //Update the data address in the slot
            memcpy(page + curr_record_start_addr, &new_addr_offset, sizeof (unsigned ));
        }
        return 0;
    }
    //Returns the pageIndex of the new Page
    int RecordBasedFileManager::insertDataNewPage(FileHandle &fileHandle, int recordSize, void* record){
        char* page = (char*)malloc(PAGE_SIZE);
        memset(page, 0, PAGE_SIZE);
        //Insert record at the start of the page
        memcpy(page, record, recordSize);

        //File structure: <start add><record len><start add><record len><N><F>

        int start_address_of_freeSpace = PAGE_SIZE - sizeof(unsigned);
        int start_address_of_slotNum = PAGE_SIZE - 2*sizeof(unsigned);
        int start_address_of_directory_lengthField = PAGE_SIZE - 3*sizeof(unsigned);
        int start_address_of_directory_AddressField = PAGE_SIZE - 4*sizeof(unsigned);
        //Always reserve 6 byte atleast because if it becomes a tombstone,
        //it will require 6 byte
        int deductible_space = std::max(recordSize, 6);
        int freeSpace = PAGE_SIZE - deductible_space - 4*sizeof(unsigned);
        int slotNum = 1;
        int startAddress = 0;
        //Store FreeSpace size
        memcpy(page + start_address_of_freeSpace, &freeSpace, sizeof(unsigned));
        //Store Number of slots
        memcpy(page + start_address_of_slotNum, &slotNum, sizeof(unsigned));
        //store length field
        memcpy(page + start_address_of_directory_lengthField, &recordSize, sizeof(unsigned));
        //store start address
        memcpy(page + start_address_of_directory_AddressField, &startAddress, sizeof(unsigned));

        //Append page
        fileHandle.appendPage(page);
        //Free page malloc
        free(page);
        int pageNums = fileHandle.getNumberOfPages();
        return pageNums - 1;
    }
    /*
     * First it will see in the last page if enough space is available
     * If not it starts looking from the first page if enough space is available
     * Return the pageIndex where the freeSpace was available
     * If not found, return -1
     * In case of not found, a new page will be appended in the caller function*/
    int RecordBasedFileManager::findFreePageIndex(FileHandle &fileHandle, int recordSize){
        int pageNums = fileHandle.getNumberOfPages();
        //Freespace has to be atleast (recordSize + 2*4 B). 2*4 B is for metadata
        int requiredSize = std::max(recordSize,6) + 2*sizeof(unsigned );
        //First look at last page
        int start_address_of_freeSpace = PAGE_SIZE - sizeof(unsigned);
        char* page = (char*)malloc(PAGE_SIZE);
        memset(page, 0, PAGE_SIZE);
        //Read last page
        fileHandle.readPage(pageNums - 1, page);
        //Read freeSpace Value
        int freeSpace = 0;
        memcpy(&freeSpace, page + start_address_of_freeSpace, sizeof(unsigned ));
        //Freespace has to be atleast requiredSize
        if(freeSpace >= requiredSize){
            free(page);
            return pageNums-1;
        }

        //Or start looking from first page
        //Don't need to check the last page
        for(int i = 0; i < pageNums - 1; i++){
            fileHandle.readPage(i, page);
            memcpy(&freeSpace, page + start_address_of_freeSpace, sizeof(unsigned ));
            if(freeSpace >= requiredSize){
                free(page);
                return i;
            }
        }
        free(page);
        //If no such page was found, return -1
        return -1;
    }

    void* RecordBasedFileManager::encoder(const std::vector<Attribute> &recordDescriptor, const void *data, int& getRecordSize){
        int numberOfCols = recordDescriptor.size();
        int bitMapSize = ceil((float)numberOfCols/8); // eg. 1 for 3 cols, 2 for 9 cols

        int N = 0;
        std::vector<bool> nullIndicator;
        for(int k = 0; k < numberOfCols; k++){
            bool isNull = isColValueNull(data, k);
            nullIndicator.push_back(isNull);
            if(isNull) continue;
            N++; // Increase N for non null value
        }

        int recordSize = calculateRecordSize(N, recordDescriptor, data, nullIndicator);
        getRecordSize = recordSize;
        void *record = malloc(recordSize);
        //Fill memory with zero
        memset(record, 0, recordSize);
        copyInputToRecord(record, data, recordDescriptor, nullIndicator, N);
        return record;
    }

    RC RecordBasedFileManager::copyInputToRecord(void* record, const void *data, const std::vector<Attribute> &recordDescriptor, const std::vector<bool> &nullIndicator, int N){
        int numberOfCols = recordDescriptor.size();
        int bitMapSize = ceil((float)numberOfCols/8); // eg. 1 for 3 cols, 2 for 9 cols
        char* pointer = (char *)data;
        int offset = 0;
        //Fill first 4 byte with value of N
        memcpy((char * )record + offset, &N, sizeof(int));
        offset += sizeof(unsigned);

        //Fill the next bitMapSize * sizeof(char) bytes with bitmap
        memcpy((char *)record + offset, pointer, bitMapSize * sizeof(char));

        offset += 4;
        //Increase pointer to next byte in the *data
        pointer = pointer + bitMapSize;

        //st, st + 4 (bitmap), N1, N2, N3, st + (1+N)*4, st +
        /*
         * First data insertion address is calculated as:
         * start address of record + 4 bytes for  N + 4 bytes for bitmap
         * + (N*4) bytes for mini folder of pointers*/
        int dataInsertionOffsetFromStartOfRecord = sizeof(int) + 4 + N * sizeof(int);
        for(int k = 0; k < numberOfCols; k++){
            bool isNull = nullIndicator.at(k);
            if(isNull) continue;
            Attribute attr = recordDescriptor.at(k);
            if(attr.type == TypeVarChar){
                int len = *(int *)pointer; //Next 4 byte gives length
                //The length info was stored in 4 byte. So, increase the pointer by 4 bytes
                //After this pointer points to value of varchar
                pointer = pointer + sizeof(int);
                /*
                 * This is not absolute, but offset. eg. if in the file, startAddressOfrecord
                 * is 2000 B, then (2000 + end_address) is where the field ends
                 * Suppose N = 3.
                 * First 4 byte in record stores N
                 * Next 4 byte in record stores bitmap = 000...
                 * Next N*4 bytes store pointers to end address
                 * So, if first col is varchar of length 3, end_address = 20 + 3 -1 = 22
                 */
                int end_address = dataInsertionOffsetFromStartOfRecord + len - 1;

                memcpy((char *)record + offset, &end_address, sizeof(unsigned));
                offset += sizeof(unsigned);

                //Insert the data at dataInsertionOffsetFromStartOfRecord
                memcpy((char *)record + dataInsertionOffsetFromStartOfRecord, pointer, len);
                dataInsertionOffsetFromStartOfRecord += len;
                pointer += len;
            } else {
                //Pointer points to 4 byte of either int or real type
                int end_address = dataInsertionOffsetFromStartOfRecord + sizeof(unsigned) - 1;
                memcpy((char *)record + offset, &end_address, sizeof(unsigned));
                offset += sizeof(unsigned);

                //Insert the data at dataInsertionOffsetFromStartOfRecord
                memcpy((char *)record +  dataInsertionOffsetFromStartOfRecord, pointer, sizeof(unsigned ));
                dataInsertionOffsetFromStartOfRecord += sizeof(unsigned);
                pointer += sizeof(unsigned);
            }
        }
        return 0;
    }

    int RecordBasedFileManager::calculateRecordSize(int N, const std::vector<Attribute> &recordDescriptor, const void *data, const std::vector<bool> &nullIndicator){

        int numberOfCols = recordDescriptor.size();
        int bitMapSize = ceil((float)numberOfCols / 8); // eg. 1 for 3 cols, 2 for 9 cols

        int recordSize = sizeof(int) + 4 + N * sizeof(int); // 4 byte for N, 4 byte for bitmap, N*4 = size of Mini directory,
        char* pointer = (char *)data + bitMapSize * sizeof(char);

        for(int k = 0; k < numberOfCols; k++){
            bool isNull = nullIndicator.at(k);
            if(isNull) continue;
            Attribute attr = recordDescriptor.at(k);
            //4 byte Length field is present only for varchar
            if(attr.type == TypeVarChar){
                unsigned len = *(int *) pointer;
                recordSize += len;
                pointer = pointer + len + 4;
            } else if(attr.type == TypeInt) {
                recordSize += sizeof(int);
                pointer += sizeof(int);
            } else {
                recordSize += sizeof(float);
                pointer += sizeof(float);
            }
        }
        return recordSize;
    }
    bool RecordBasedFileManager::isColValueNull(const void *data, int k){
        return (bool)(*((unsigned char *) data + k/8) & (1 << (8 - k%8 - 1)));
        unsigned char* pointer;
        int multiplier = (k / 8); // 0 for k = 2, 1 for k = 8
        int offset = (k % 8 + 1); // 3 for k = 2, 1 for k = 8
        pointer = (unsigned char *)data;
        //shift pointer by multiplier;
        pointer = pointer + multiplier; // shifted to second byte of data for k = 8
        unsigned char bitMap = *pointer;
        unsigned char bitMask = 1 << (8 - offset);
        int isNull = bitMap & bitMask; // If 1, then null
        if(isNull == 1) return true;
        return false;
    }


    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        int pageIndex = rid.pageNum;
        int slotNum = rid.slotNum;
        char* page = (char*)malloc(PAGE_SIZE);
        memset(page, 0, PAGE_SIZE);
        fileHandle.readPage(pageIndex, page);
        int offsetForStartAddress = getStartAddressOffset(slotNum);
        int offsetForRecordLength = getLenAddressOffset(slotNum);

        int numSlots = 0;
        memcpy(&numSlots, page + PAGE_SIZE - 2*sizeof (unsigned ) , sizeof (unsigned ));
        int startAddress = 0;
        int lengthOfRecord = 0;

        memcpy(&startAddress, page + offsetForStartAddress, sizeof(unsigned ));
        memcpy(&lengthOfRecord, page + offsetForRecordLength, sizeof(unsigned ));
        //This record was deleted, so cant be read
        if(lengthOfRecord == -1){
            free(page);
            return -1;
        }
        bool tombstone_flag = isTombStone(startAddress);
        if(tombstone_flag){
            RID next_rid;
            tombStonePointerExtractor(next_rid, startAddress, page);
            readRecord(fileHandle, recordDescriptor, next_rid, data);
        }else{
            bool internal_id_flag = isInternalId(startAddress);
            if(internal_id_flag){
                lengthOfRecord = lengthOfRecord - 6*sizeof (char);
            }
            startAddress = startAddress & ((1 << 17) - 1);
            void* record = malloc(lengthOfRecord);
            memcpy(record, page + startAddress, lengthOfRecord);
            decoder(recordDescriptor, record, data);
            free(record);
        }
        free(page);
        return 0;
    }

    void* RecordBasedFileManager::decoder(const std::vector<Attribute> &recordDescriptor, void* record, void *data){

        int numberOfCols = recordDescriptor.size();
        int bitMapSize = ceil((float)numberOfCols/8);
        //Get the number of nonnull value from first four bytes
        int N = *(int *) record;
        std::vector<bool> nullIndicator;
        int bitMap = 0;
        //The next 4 bytes after N contains bitMap
        memcpy(&bitMap, (int*) record + 1, sizeof(unsigned));

        for(int k = 0; k < numberOfCols; k++){
            bool isNull = isColValueNull(&bitMap, k);
            nullIndicator.push_back(isNull);
        }
        int sizeOfData = calculateDataSize(recordDescriptor, record, nullIndicator);
        //Assign memory for data
//        void* data = malloc(sizeOfData);
        //Fill the memory with zeros
        memset(data, 0, sizeOfData);
        copyRecordToData(recordDescriptor, data, record, nullIndicator);
        return data;
    }

    RC RecordBasedFileManager::copyRecordToData(const std::vector<Attribute> &recordDescriptor, void* data, void* record, const std::vector<bool> &nullIndicator){
        int numberOfCols = recordDescriptor.size();
        int bitMapSize = ceil((float)numberOfCols/8);
        //Get the number of nonnull value from first four bytes
        int N = *(int *) record;
        //Copy bitMap
        int dataOffset = 0;
        //Copy the bitmap of bitMapSize
        memcpy((char*) data + dataOffset, (char*) record + sizeof(int), bitMapSize);

        int *minidict = (int *) ((char *)record + sizeof(int) + 4);
        int numNonNullCols = 0;

        //Increase dataOffset to end of bitMap
        dataOffset += bitMapSize;
        int dataInsertionOffsetFromStartOfRecord = (2 + N)*4;
        for(int k = 0; k < nullIndicator.size(); k++){
            bool isNull = nullIndicator.at(k);
            if(isNull) continue;
            Attribute attr = recordDescriptor.at(k);
            if(attr.type == TypeVarChar){
                int end_address_offset = minidict[numNonNullCols];
                int len = end_address_offset - dataInsertionOffsetFromStartOfRecord + 1;
                //Save the length in the 4 bytes of data
                memcpy((char *) data + dataOffset, &len, sizeof(unsigned));
                dataOffset += sizeof(unsigned);
                //Mira
                memcpy((char *)data + dataOffset, (char *) record + dataInsertionOffsetFromStartOfRecord, len);
                dataInsertionOffsetFromStartOfRecord += len;
                dataOffset += len;
            } else {
                //No need to store size info for int and real type
                memcpy((char *)data + dataOffset, (char *)record + dataInsertionOffsetFromStartOfRecord, sizeof(unsigned));
                dataOffset += sizeof(unsigned);
                dataInsertionOffsetFromStartOfRecord += sizeof(unsigned);
            }
            numNonNullCols += 1;
        }
        return 0;
    }
    int RecordBasedFileManager::calculateDataSize(const std::vector<Attribute> &recordDescriptor, void* record, const std::vector<bool> &nullIndicator){
        int sizeOfData = 0;
        int numberOfCols = recordDescriptor.size();
        int bitMapSize = ceil((float)numberOfCols/8);

        sizeOfData += bitMapSize;
        //Get the number of nonnull value from first four bytes
        int N = *(int *) record;

        //offset to the first box of mini directory
        int *minidict = (int *) ((char *)record + sizeof(int) + 4);
        int numNonNullCols = 0;

        int dataInsertionOffsetFromStartOfRecord = (2 + N)*4;
        for(int k = 0; k < numberOfCols; k++){
            bool isNull = nullIndicator.at(k);
            if(isNull) continue;
            Attribute attr = recordDescriptor.at(k);
            if(attr.type == TypeVarChar){
                //append length size to data size
                int end_address_offset = minidict[numNonNullCols];
                int len = end_address_offset - dataInsertionOffsetFromStartOfRecord + 1;
                sizeOfData = sizeOfData + len;
                //append 4 byte for storing this length size in data
                sizeOfData += sizeof(unsigned);
                //Increase dataInsertionOffsetFromStartOfRecord offset by length of varchar
                dataInsertionOffsetFromStartOfRecord += len;
            } else {
                //For int and real type, only value will be added in the
                //data and no length information. So, just add 4 [size of value]
                sizeOfData += sizeof(unsigned);
                //Increase dataInsertionOffsetFromStartOfRecord offset by length of 4
                dataInsertionOffsetFromStartOfRecord += sizeof(unsigned);
            }
            numNonNullCols += 1;
        }
        return sizeOfData;
    }


    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        int pageIndex = rid.pageNum;
        int slotNum = rid.slotNum;
        char* page = (char*)malloc(PAGE_SIZE);
        memset(page, 0, PAGE_SIZE);
        fileHandle.readPage(pageIndex, page);

        int data_address_offset = getStartAddressOffset(slotNum);
        int record_address = *(int *)(page + data_address_offset);

        //Check if tombstone
        bool tombstone_flag = isTombStone(record_address);

        if(tombstone_flag){
            RID next_rid;
            tombStonePointerExtractor(next_rid, record_address, page);
            deleteRecord(fileHandle, recordDescriptor, next_rid);
        }
        /*
         * It does not matter if it is internal id or not,
         * the deleteGivenRecord function will simply delete it*/
        deleteGivenRecord(fileHandle, recordDescriptor, rid);
        return 0;
    }

    RC RecordBasedFileManager::deleteGivenRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        int pageIndex = rid.pageNum;
        int slotNum = rid.slotNum;
        char* page = (char*)malloc(PAGE_SIZE);
        memset(page, 0, PAGE_SIZE);
        fileHandle.readPage(pageIndex, page);

        int numSlots = 0;
        memcpy(&numSlots, page + PAGE_SIZE - 2*sizeof (unsigned ) , sizeof (unsigned ));
        int freeSpace = 0;
        memcpy(&freeSpace, page + PAGE_SIZE - sizeof (unsigned ) , sizeof (unsigned ));
        int data_address_offset = getStartAddressOffset(slotNum);
        int len_address_offset = getLenAddressOffset(slotNum);
        //Length of this record
        int deleted_record_len = 0;
        memcpy(&deleted_record_len, page + len_address_offset, sizeof (unsigned )); //pass
        /*Put -1 in the length and start address of this record
         *
         * */
        int recordDeleted = -1;
        memcpy(page + len_address_offset, &recordDeleted, sizeof (unsigned ));
        memcpy(page + data_address_offset, &recordDeleted, sizeof (unsigned ));

        /*
         * Move the records at the right of this to left.
         * Update free space
         * */
        shiftRecordsLeft(page, numSlots, slotNum + 1, deleted_record_len);

        //Update freespace field
        freeSpace = freeSpace - deleted_record_len;
        memcpy(page + PAGE_SIZE - sizeof (unsigned ), &freeSpace, sizeof (unsigned));
        fileHandle.writePage(pageIndex, page);
        free(page);
        return 0;
    }

    /*
     * Returns the start of data field address for given slot number
     * 1 for F, 1 for N, 2*SlotNum for each slot
     * */
    int RecordBasedFileManager::getStartAddressOffset(int slotNum){
        return (PAGE_SIZE - (2 + 2*slotNum)*sizeof (unsigned ));
    }

    /*
     * Returns the start of length field address for given slot number
     * */
    int RecordBasedFileManager::getLenAddressOffset(int slotNum){
        return (PAGE_SIZE - (2 + 2*slotNum - 1)*sizeof (unsigned ));
    }
    //If slot is empty, the length is -1, return true
    //else return false
    bool RecordBasedFileManager::is_slot_empty(char* page, int slotNum){
        int len = 0;
        memcpy(&len, page + PAGE_SIZE - (2 + 2*slotNum - 1)*sizeof (unsigned ), sizeof (unsigned ));
        if(len == -1) return true;
        return false;
    }
    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        int numberOfCols = recordDescriptor.size();
        int bitMapSize = ceil((float)numberOfCols/8); // eg. 1 for 3 cols, 2 for 9 cols
        char* pointer = (char *)data;

        int N = 0;
        std::vector<bool> nullIndicator;

        for(int k = 0; k < numberOfCols; k++){
            bool isNull = isColValueNull(data, k);
            nullIndicator.push_back(isNull);
            if(isNull) continue;
            N++; //Increase N for non null value
        }
        pointer = pointer + bitMapSize;
        int offset = bitMapSize;
//        std::string str = "";
        //<AttributeName1>:\s<Value1>,\s<AttributeName2>:\s<Value2>,\s<AttributeName3>:\s<Value3>\n
        for(int k = 0; k < numberOfCols; k++){
            Attribute attr = recordDescriptor.at(k);
            bool isNull = nullIndicator.at(k);
//            str.append(attr.name);
//            str.append(": ");
            out << attr.name << ": ";
            if(isNull) {
//                str.append("NULL");
                out << "NULL";
                if(k < numberOfCols - 1) {
//                    str.append(", ");
                    out << ", ";
                }
                continue;
            }
            if(attr.type == TypeVarChar){
                int len = *(int *)pointer; //Next 4 byte gives length
                //The length info was stored in 4 byte. So, increase the pointer by 4 bytes
                //After this pointer points to value of varchar
                pointer += sizeof(int);
                offset += sizeof(int);
                if(len > 0) {
                    char array[len + 1];
                    memcpy(array, pointer, len * sizeof(char));
                    pointer += len * sizeof(char);
                    offset += len * sizeof(char);
                    array[len] = '\0';
//                str += array;
//                str.append(array);
                    out << array;
                }
            } else if(attr.type == TypeInt){
//                int val = *(int*) pointer;
                int val;
                memcpy(&val, pointer, sizeof(int));
//                str += std::to_string(val);
//                str.append(std::to_string(val));
                out << val;
                pointer += sizeof(int);
                offset += sizeof(int);
            } else {
//                float val = *(float*)pointer;
                float val;
                memcpy(&val, pointer, sizeof(float));
//                str += std::to_string(val);
//                str.append(std::to_string(val));
                out << val;
                pointer += sizeof(float);
                offset += sizeof(float);
            }
            if(k < numberOfCols) {
//                str.append(", ");
                out << ", ";
            } else {
//                str.append("\n");
                out << "\n";
            }
        }
//        out << str;
//        out.write((char *)&str, sizeof(str));
        return 0;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {

        int updatedRecordSize = 0;
        //It will store the size of the record in the recordSize variable
        void * record = encoder(recordDescriptor, data, updatedRecordSize);

        updateMiscRecord(fileHandle, recordDescriptor, record, updatedRecordSize, rid);
        return 0;
    }

    RC RecordBasedFileManager::updateMiscRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, void *updated_record, int updatedRecordSize, const RID &rid) {
        int pageIndex = rid.pageNum;
        int slotNum = rid.slotNum;
        char* page = (char*)malloc(PAGE_SIZE);
        memset(page, 0, PAGE_SIZE);
        fileHandle.readPage(pageIndex, page);

        int len_address = getLenAddressOffset(slotNum);
        int data_start_address = getStartAddressOffset(slotNum);
        int record_old_len = 0;
        int record_data_addr = 0;
        memcpy(&record_old_len, page + len_address, sizeof (unsigned ));
        memcpy(&record_data_addr, page + data_start_address, sizeof (unsigned ));
        if(isTombStone(record_data_addr)){
            RID next_rid;
            tombStonePointerExtractor(next_rid, record_data_addr, page);
            updateMiscRecord(fileHandle, recordDescriptor, updated_record, updatedRecordSize, next_rid);
        }
        bool is_internal = isInternalId(record_data_addr);

        record_data_addr = record_data_addr & ((1 << 17) - 1);

        int numSlots = 0;
        memcpy(&numSlots, page + PAGE_SIZE - 2*sizeof (unsigned ) , sizeof (unsigned ));
        int freeSpace = 0;
        memcpy(&freeSpace, page + PAGE_SIZE - sizeof (unsigned ) , sizeof (unsigned ));

        /*
         * This is a simple case
         * If the updated record size id less than the old size
         * Just update the record, update the length data
         * and shift all the other record to left(Similar to delete record).
         * No change in internal id flag of the address, as it is still an internal id
         */
        if(updatedRecordSize < record_old_len){
            //Add data
            memcpy(page + record_data_addr, updated_record, updatedRecordSize);
            //Update length field
            memcpy(page + len_address, &updatedRecordSize, sizeof (unsigned ));
            //shift all other data to left
            shiftRecordsLeft(page, numSlots ,slotNum + 1, record_old_len - updatedRecordSize);
            //Update free space
            freeSpace = freeSpace + (record_old_len - updatedRecordSize);
            memcpy(page + PAGE_SIZE - sizeof (unsigned ), &freeSpace, sizeof (unsigned));
            fileHandle.writePage(pageIndex, page);
        }else{
            /*
             * This is a trickier one. It has two cases.
             * a. If the free space in the page is enough to accommodate it,
             * then it is easy. Simply rightshift all other record and insert this. No change in internal id flag of the address, as it is still an internal id
             * b. This case is tricky. If there is not enough space, we need to find another page and mark this slot as tombstone. In this case if it was an internal id, we need to remove the internal id flag other than adding a tombstone flag.
             * */
            if(updatedRecordSize - record_old_len <= freeSpace){
                shiftRecordsRight(page, numSlots, slotNum + 1, updatedRecordSize - record_old_len);
                //Now add the record
                memcpy(page + record_data_addr, updated_record, updatedRecordSize);
                //Update length field
                memcpy(page + len_address, &updatedRecordSize, sizeof (unsigned ));
                //Update free space
                freeSpace = freeSpace - (updatedRecordSize - record_old_len);
                memcpy(page + PAGE_SIZE - sizeof (unsigned ), &freeSpace, sizeof (unsigned));
                fileHandle.writePage(pageIndex, page);
            }else{
                //The tricky case

                /*
                 * ToDo: Add record id info to this data*/
                //check if it was previously stored in an internal id
                RID original_rid;
                char* internal_old_record;
                if(is_internal){
                    internalRecordExtractor(original_rid, record_data_addr, record_old_len, page, internal_old_record);
                }else{
                    original_rid.pageNum = pageIndex;
                    original_rid.slotNum = slotNum;
                }
                RID insert_rid;
                //Add the original RID values to this record
                char* rid_added_updated_record = (char*)malloc(updatedRecordSize + 6*sizeof (char));
                memset(rid_added_updated_record, 0, updatedRecordSize + 6*sizeof (char));
                memcpy(rid_added_updated_record, updated_record, updatedRecordSize);
                memcpy(rid_added_updated_record + updatedRecordSize, &original_rid.pageNum, sizeof (unsigned ));
                memcpy(rid_added_updated_record + updatedRecordSize + sizeof (unsigned ), &original_rid.slotNum, 2*sizeof (char));
                insertMiscData(fileHandle, recordDescriptor, rid_added_updated_record, updatedRecordSize + 6*sizeof (char), insert_rid);
                /*
                 * Update the slot inserted to indicate that it's
                 * an internal flag*/
                int insert_pageNum = insert_rid.pageNum;
                int insert_slotNum = insert_rid.slotNum;
                char* insert_page = (char*)malloc(PAGE_SIZE);
                memset(insert_page, 0, PAGE_SIZE);
                fileHandle.readPage(insert_pageNum, insert_page);


                int addr = getStartAddressOffset(insert_slotNum);
                int record_addr = *(int *)(insert_page + addr);
                record_addr = record_addr | (1 << 30);
                memcpy(insert_page + addr, &record_addr, sizeof (unsigned));


                fileHandle.writePage(insert_pageNum, insert_page);
                free(insert_page);
                //Store the insert page num and slot num info here
                //This page will fo sure have the size available for
                //this update. Not calling updateRecord function here, because the data is not the format of *data
                //So, do a insertion from scratch

                //Create data
                int tombStone_data_len = 6*sizeof(char);
                char* tombStoneData = (char*)malloc(tombStone_data_len);
                memcpy(tombStoneData, &insert_pageNum, sizeof(unsigned ));
                memcpy(tombStoneData + sizeof (unsigned ), &insert_slotNum, 2*sizeof (char));
                updateMiscRecord(fileHandle, recordDescriptor, tombStoneData, 6*sizeof (char), rid);
                /*
                 * We need to remove the internal id flag if it was an internal id as now this will be a tombstone only
                 * a. Internal id flag removal: no action as record_data_addr & ((1 << 17) - 1) took care of it
                 * b. Add tombstone flag
                 * Length field update was taken care of because it called updateMiscRecord*/
                fileHandle.readPage(pageIndex, page);
                record_data_addr = record_data_addr |(1 << 31);
                memcpy(page + data_start_address, &record_data_addr, sizeof (unsigned ));
                fileHandle.writePage(pageIndex, page);
                free(page);
                free(tombStoneData);
                //free(rid_added_updated_record);

            }

        }

        return 0;
    }

    bool RecordBasedFileManager::isTombStone(int address){
        if((address & (1 << 31)) == 0) return false;
        return true;
    }
    bool RecordBasedFileManager::isInternalId(int address){
        if((address & (1 << 30)) == 0) return false;
        return true;
    }
    RC RecordBasedFileManager::tombStonePointerExtractor(RID &rid, int addr, char* page){
        addr = addr & ((1 << 17) - 1);
        memcpy(&rid.pageNum, page+ addr,sizeof (unsigned ));
        memcpy(&rid.slotNum,page+addr+sizeof (unsigned ),2*sizeof (char));
        return 0;
    }
    RC RecordBasedFileManager::internalRecordExtractor(RID &real_rid, int addr, int len, char* page, char* record){
        addr = addr & ((1 << 17) - 1);
        //The actual record
        memcpy(record, page + addr, len - 6);
        //Copy the page number
        memcpy(&real_rid.pageNum, page + addr + len - sizeof (unsigned ) - 2*sizeof (char ), sizeof (unsigned ));
        //Copy the slot number
        memcpy(&real_rid.slotNum, page + addr + len - 2*sizeof (char ), 2*sizeof (char));
        return 0;
    }
    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        void* data_record;
        //Read the record given by rid
        readRecord(fileHandle, recordDescriptor, rid, data_record);
        int numberOfCols = recordDescriptor.size();
        int bitMapSize = ceil((float)numberOfCols/8);

        std::vector<bool> nullIndicator;
        for(int k = 0; k < numberOfCols; k++){
            bool isNull = isColValueNull(data, k);
            nullIndicator.push_back(isNull);
            if(isNull) continue;
        }
        bool is_null = false;
        int index_of_asked_attribute;

        //check if the asked attributeName value is null
        for(int i = 0; i < numberOfCols; i++){
            Attribute attr = recordDescriptor.at(i);
            if(strcmp(attr.name.c_str(), attributeName.c_str()) == 0){
                index_of_asked_attribute = i;
                if(isColValueNull(data_record, i)){
                    is_null = true;
                }
                break;
            }
        }
        if(is_null){
            data = malloc(sizeof (char));
            memset(data, 0, sizeof (char));
            //attribute is null
            char bitmap = char(128);
            memcpy(data, &bitmap, sizeof (char));
            return -1;
        }
        //the attribute value is not null
        char* pointer = (char*)data_record;
        int offset = bitMapSize;

        for(int j = 0; j <= index_of_asked_attribute; j++){
            if(j == index_of_asked_attribute) break;
            if(recordDescriptor.at(j).type != TypeVarChar){
                offset += sizeof (unsigned );
            }else{
                int len = *(int *)(pointer + offset);
                offset += sizeof (unsigned);
                offset += len;
            }
        }
        if(recordDescriptor.at(index_of_asked_attribute).type != TypeVarChar){
            data = malloc(sizeof (char ) + sizeof (unsigned ));
            memset(data, 0, sizeof (char ) + sizeof (unsigned ));
            memcpy((char*)data + sizeof (char), (char*) pointer + offset, sizeof (unsigned ));
        }else{
            int len_of_data = *(int*)(pointer + offset);
            offset += sizeof (unsigned );
            data = malloc(sizeof (char) + sizeof (unsigned ) + len_of_data);
            memset(data, 0, sizeof (char) + sizeof (unsigned ) + len_of_data);
            memcpy((char*)data + sizeof (char), &len_of_data, sizeof (unsigned ));
            memcpy((char*)data + sizeof (char) + sizeof (unsigned ), (char*)pointer + offset, len_of_data);
        }
        return 0;
    }
    //Returns an iterator called rbfm_ScanIterator
    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {

        rbfm_ScanIterator.setScanner(fileHandle, recordDescriptor, conditionAttribute, compOp, value);
        RID rid;
        void* data;
        while(rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF){

        }
        return 0;
    }

    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {

        int totalPages = fileHandle.getNumberOfPages();
        if(totalPages == 0) return RBFM_EOF;
        currSlotNum++;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        int totalSlots = 0;
        memcpy(&totalSlots, page + PAGE_SIZE - 2*sizeof (unsigned ) , sizeof (unsigned ));
        if(currSlotNum > totalSlots){
            currPageIndex++;
            if(currPageIndex + 1 > totalPages){
                return RBFM_EOF;
            }
            currSlotNum = 1;
            fileHandle.readPage(currPageIndex, page);
            rid.pageNum = currPageIndex;
            rid.slotNum = currSlotNum;
            rbfm.readRecord(fileHandle, recordDescriptor, rid, data);
        }else{
            rid.pageNum = currPageIndex;
            rid.slotNum = currSlotNum;
            rbfm.readRecord(fileHandle, recordDescriptor, rid, data);
        }
        return 0;
    }

    RC RBFM_ScanIterator::close() {
        free(page);
        return 0;
    }
    RC RBFM_ScanIterator::setScanner(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const std::string &conditionAttribute
            , const CompOp compOp, const void *value){
        this->fileHandle = fileHandle;
        this->recordDescriptor = recordDescriptor;
        this->compOp = compOp;
        this->value = value;
        this->currPageIndex = -1;
        this->currSlotNum = 0;
        this->page = (char*)malloc(PAGE_SIZE);
        memset(page, 0, PAGE_SIZE);
        if(fileHandle.getNumberOfPages() != 0){
            currPageIndex++;
            fileHandle.readPage(currPageIndex, page);
        }
    }

} // namespace PeterDB

