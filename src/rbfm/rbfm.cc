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

