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
    //Returns slotNum where record was inserted
    int RecordBasedFileManager::insertDataByPageIndex(FileHandle &fileHandle, int pageIndex, void* record, int recordSize){

        char* page =(char *) malloc(PAGE_SIZE);
        fileHandle.readPage(pageIndex, page);
        int start_address_of_slotNum = PAGE_SIZE - 2*sizeof(unsigned);
        //Get Number of slots
        unsigned numOfSlots = *(unsigned *)(page + start_address_of_slotNum);
//        memcpy(&numOfSlots, page + start_address_of_slotNum, sizeof(unsigned ));

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

        /*Update free space*/
        //First get the freeSpace val
        int start_address_of_freeSpace = PAGE_SIZE - sizeof(unsigned);
        int freeSpace = 0;
        memcpy(&freeSpace, page + start_address_of_freeSpace, sizeof(unsigned));
        freeSpace = freeSpace - 2*sizeof(unsigned) - recordSize;
        memcpy(page + start_address_of_freeSpace, &freeSpace, sizeof(unsigned));
        fileHandle.writePage(pageIndex, page);
        free(page);
        return numOfSlots;
    }
    //Returns the pageIndex of the new Page
    int RecordBasedFileManager::insertDataNewPage(FileHandle &fileHandle, int recordSize, void* record){
        char* page = (char*)malloc(PAGE_SIZE);
        //Insert record at the start of the page
        memcpy(page, record, recordSize);

        //File structure: <start add><record len><start add><record len><N><F>

        int start_address_of_freeSpace = PAGE_SIZE - sizeof(unsigned);
        int start_address_of_slotNum = PAGE_SIZE - 2*sizeof(unsigned);
        int start_address_of_directory_lengthField = PAGE_SIZE - 3*sizeof(unsigned);
        int start_address_of_directory_AddressField = PAGE_SIZE - 4*sizeof(unsigned);

        int freeSpace = PAGE_SIZE - recordSize - 4*sizeof(unsigned);
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
        int requiredSize = recordSize + 2*sizeof(unsigned );
        //First look at last page
        int start_address_of_freeSpace = PAGE_SIZE - sizeof(unsigned);
        char* page = (char*)malloc(PAGE_SIZE);
        //Read last page
        fileHandle.readPage(pageNums - 1, page);
        //Read freeSpace Value
        int freeSpace = 0;
        memcpy(&freeSpace, page + start_address_of_freeSpace, sizeof(unsigned ));
        //Freespace has to be atleast requiredSize
        if(freeSpace >= requiredSize) return pageNums-1;

        //Or start looking from first page
        //Don't need to check the last page
        for(int i = 0; i < pageNums - 1; i++){
            fileHandle.readPage(i, page);
            memcpy(&freeSpace, page + start_address_of_freeSpace, sizeof(unsigned ));
            if(freeSpace >= requiredSize) return i;
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
        fileHandle.readPage(pageIndex, page);
        int numSlots = 0;
        memcpy(&numSlots, page + PAGE_SIZE - 2*sizeof (unsigned ) , sizeof (unsigned ));
        int startAddress = 0;
        int lengthOfRecord = 0;
        int offsetForStartAddress = PAGE_SIZE - 2 * sizeof(unsigned ) - 2*slotNum * sizeof(unsigned );
        int offsetForRecordLength = offsetForStartAddress + sizeof(unsigned );
        memcpy(&startAddress, page + offsetForStartAddress, sizeof(unsigned ));
        memcpy(&lengthOfRecord, page + offsetForRecordLength, sizeof(unsigned ));
        void* record = malloc(lengthOfRecord);
        memcpy(record, page + startAddress, lengthOfRecord);
        decoder(recordDescriptor, record, data);
        free(record);
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
        return -1;
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
        std::string str = "";
        //<AttributeName1>:\s<Value1>,\s<AttributeName2>:\s<Value2>,\s<AttributeName3>:\s<Value3>\n
        for(int k = 0; k < numberOfCols; k++){
            Attribute attr = recordDescriptor.at(k);
            bool isNull = nullIndicator.at(k);
            str.append(attr.name);
            str.append(": ");
            if(isNull) {
                str.append("NULL");
                if(k < numberOfCols - 1) str.append(", ");
                continue;
            }
            if(attr.type == TypeVarChar){
                int len = *(int *)pointer; //Next 4 byte gives length
                //The length info was stored in 4 byte. So, increase the pointer by 4 bytes
                //After this pointer points to value of varchar
                pointer += sizeof(int);
                offset += sizeof(int);
                char array[len];
                memcpy(array, pointer, len * sizeof(char));
                pointer += len * sizeof(char);
                offset += len * sizeof(char);
                str += array;
//                str.append(array);
            } else if(attr.type == TypeInt){
//                int val = *(int*) pointer;
                int val;
                memcpy(&val, pointer, sizeof(int));
                str += std::to_string(val);
//                str.append(std::to_string(val));
                pointer += sizeof(int);
                offset += sizeof(int);
            } else {
//                float val = *(float*)pointer;
                float val;
                memcpy(&val, pointer, sizeof(float));
                str += std::to_string(val);
//                str.append(std::to_string(val));
                pointer += sizeof(float);
                offset += sizeof(float);
            }
            if(k < numberOfCols) str.append(", ");
            else str.append("\n");
        }
        out << str;
//        out.write((char *)&str, sizeof(str));
        return 0;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }

} // namespace PeterDB

