#include "src/include/rbfm.h"
#include <cmath>
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
        void * record = encoder(recordDescriptor, data);
        int pageNums = fileHandle.getNumberOfPages();
        //Page num in which data finally got inserted
        int insertPageNum;
        /*
         * If pageNums is zero, then this is the first record and
         * we need to append a page*/

        if(pageNums == 0){
            fileHandle.appendPage(record);
            insertPageNum = 0;
            //Add F: free space
            //Add N: Number of record
            //Add recordsize and start_address slot
        }

        return -1;
    }



    void* RecordBasedFileManager::encoder(const std::vector<Attribute> &recordDescriptor, const void *data){
        int numberOfCols = recordDescriptor.size();
        int bitMapSize = ceil(numberOfCols/8); // eg. 1 for 3 cols, 2 for 9 cols

        unsigned char* pointer;
        int N = 0;
        std::vector<bool> nullIndicator;
        for(int k = 0; k < numberOfCols; k++){
            bool isNull = isColValueNull(data, k);
            nullIndicator.push_back(isNull);
            if(isNull) continue;
            N++; //Increase N for non null value
        }

        int recordSize = calculateRecordSize(N, recordDescriptor, data,nullIndicator);
        void *record = malloc(recordSize);
        //Fill memory with zero
        memset(record, 0, recordSize);
        copyInputToRecord(record, data, recordDescriptor, nullIndicator, N);
        return record;
    }

    RC RecordBasedFileManager::copyInputToRecord(void* record, const void *data, const std::vector<Attribute> &recordDescriptor, const std::vector<bool> &nullIndicator, int N){
        int numberOfCols = recordDescriptor.size();
        int bitMapSize = ceil(numberOfCols/8); // eg. 1 for 3 cols, 2 for 9 cols
        char* pointer = (char *)data;
        int offset = 0;
        //Fill first 4 byte with value of N
        memcpy((char * )record+offset, &N, sizeof(unsigned));
        //Increase offset on record by 4 B
        offset += sizeof(unsigned);

        //Fill the next bitMapSize*sizeof(char) bytes with bitmap
        memcpy((char *)record + offset, pointer, bitMapSize*sizeof(char));

        offset += sizeof(unsigned );
        //Increase pointer to next byte in the *data
        pointer = pointer + bitMapSize;

        //st, st + 4 (bitmap), N1, N2, N3, st + (1+N)*4, st +
        /*
         * First data insertion address is calculated as:
         * start address of record + 4 bytes for  N + 4 bytes for bitmap
         * + (N*4) bytes for mini folder of pointers*/
        int dataInsertionOffsetFromStartOfRecord = (2 + N)*4;
        for(int k = 0; k < numberOfCols; k++){
            bool isNull = nullIndicator.at(k);
            if(isNull) continue;
            Attribute attr = recordDescriptor.at(k);
            if(attr.type == TypeVarChar){
                int len = *(int *)pointer; //Next 4 byte gives length
                //The length info was stored in 4 byte. So, increase the pointer by 4 bytes
                //After this pointer points to value of varchar
                pointer = pointer + 4;
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
                memcpy((char *)record +  dataInsertionOffsetFromStartOfRecord, pointer, len);
                dataInsertionOffsetFromStartOfRecord += len;
                pointer += len;
            }else{
                //Pointer points to 4 byte of either int or real type
                int end_address = dataInsertionOffsetFromStartOfRecord + sizeof(unsigned) - 1;
                memcpy((char *)record + offset, &end_address, sizeof(unsigned ));
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
        int bitMapSize = ceil(numberOfCols/8); // eg. 1 for 3 cols, 2 for 9 cols

        int recordSize = 4 * (N+2); // 4 byte for N, 4 byte for bitmap, N*4 = size of Mini directory,
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
            }else{
                recordSize += 4;
                //Move pointer by 4 byte
                pointer += 4;
            }
        }
        return recordSize;
    }
    bool RecordBasedFileManager::isColValueNull(const void *data, int k){

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
        return -1;
    }

    void* RecordBasedFileManager::decoder(const std::vector<Attribute> &recordDescriptor, void* record){

        int numberOfCols = recordDescriptor.size();
        int bitMapSize = ceil(numberOfCols/8);
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
        void* data = malloc(sizeOfData);
        //Fill the memory with zeros
        memset(data, 0, sizeOfData);
        copyRecordToData(recordDescriptor, data, record, nullIndicator);
        return data;
    }

    RC RecordBasedFileManager::copyRecordToData(const std::vector<Attribute> &recordDescriptor, void* data, void* record, const std::vector<bool> &nullIndicator){
        int numberOfCols = recordDescriptor.size();
        int bitMapSize = ceil(numberOfCols/8);
        //Get the number of nonnull value from first four bytes
        int N = *(int *) record;
        //Set offset to start of bitMap
        int recordOffset = sizeof(unsigned);
        //Copy bitMap
        int dataOffset = 0;
        //Copy the bitmap of bitMapSize
        memcpy((char*) data + dataOffset, (char*) record + recordOffset, bitMapSize);

        //Increase record offset to next 4 byte
        recordOffset += sizeof(unsigned);
        //Increase dataOffset to end of bitMap
        dataOffset += bitMapSize;
        int dataInsertionOffsetFromStartOfRecord = (2 + N)*4;
        for(int k = 0; k < nullIndicator.size(); k++){
            bool isNull = nullIndicator.at(k);
            if(isNull) continue;
            Attribute attr = recordDescriptor.at(k);
            if(attr.type == TypeVarChar){
                int end_address_offset = *((int *) record + recordOffset);
                int len = end_address_offset - dataInsertionOffsetFromStartOfRecord + 1;
                //Save the length in the 4 bytes of data
                memcpy((char *) data + dataOffset, &len, sizeof(unsigned));
                //Increase dataOffset by 4 bytes
                dataOffset += sizeof(unsigned);
                //copy
                memcpy((char *)data + dataOffset, (char *) record + dataInsertionOffsetFromStartOfRecord, len);
                //Increase dataInsertionOffset by length of varchar
                dataInsertionOffsetFromStartOfRecord += len;
                dataOffset += len;

            }else{
                //No need to store size info for int and real type
                memcpy((char *)data + dataOffset, (char *)record + dataInsertionOffsetFromStartOfRecord, sizeof(unsigned));
                dataOffset += sizeof(unsigned);
                dataInsertionOffsetFromStartOfRecord += sizeof(unsigned);
            }
            recordOffset += sizeof(unsigned);
        }
        return 0;
    }
    int RecordBasedFileManager::calculateDataSize(const std::vector<Attribute> &recordDescriptor, void* record, const std::vector<bool> &nullIndicator){
        int sizeOfData = 0;
        int numberOfCols = recordDescriptor.size();
        int bitMapSize = ceil(numberOfCols/8);

        sizeOfData += bitMapSize;
        //Get the number of nonnull value from first four bytes
        int N = *(int *) record;

        //offset to the first box of mini directory
        int offset = 2 * sizeof(unsigned);
        int dataInsertionOffsetFromStartOfRecord = (2 + N)*4;
        for(int k = 0; k < numberOfCols; k++){
            bool isNull = nullIndicator.at(k);
            if(isNull) continue;
            Attribute attr = recordDescriptor.at(k);
            if(attr.type == TypeVarChar){
                //append length size to data size
                int end_address_offset = *((int *) record + offset);
                int len = end_address_offset - dataInsertionOffsetFromStartOfRecord + 1;
                sizeOfData = sizeOfData + len;
                //append 4 byte for storing this length size in data
                sizeOfData += sizeof(unsigned);
                //Increase dataInsertionOffsetFromStartOfRecord offset by length of varchar
                dataInsertionOffsetFromStartOfRecord += len;
            }else{
                //For int and real type, only value will be added in the
                //data and no length information. So, just add 4 [size of value]
                sizeOfData += sizeof(unsigned);
                //Increase dataInsertionOffsetFromStartOfRecord offset by length of 4
                dataInsertionOffsetFromStartOfRecord += sizeof(unsigned);
            }
            //Increase offset by 4 bytes
            offset += sizeof(unsigned);
        }
        return sizeOfData;
    }


    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        return -1;
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

