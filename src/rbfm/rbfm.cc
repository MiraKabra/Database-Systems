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


    RC RecordBasedFileManager::encoder(const std::vector<Attribute> &recordDescriptor, const void *data, const void *encoded_data){
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

    }

    RC copyInputToRecord(){

    }

    int RecordBasedFileManager::calculateRecordSize(int N, const std::vector<Attribute> &recordDescriptor, const void *data, std::vector<bool> &nullIndicator){

        int numberOfCols = recordDescriptor.size();
        int bitMapSize = ceil(numberOfCols/8); // eg. 1 for 3 cols, 2 for 9 cols

        int recordSize = 4 * (N+1); // N*4 = size of Mini directory, 4 byte for bitmap
        char* pointer = (char *)data + bitMapSize;

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

