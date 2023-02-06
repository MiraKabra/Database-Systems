#ifndef _rbfm_h_
#define _rbfm_h_

#include <vector>

#include "pfm.h"

namespace PeterDB {
    // Record ID
    typedef struct {
        unsigned pageNum;           // page number
        unsigned short slotNum;     // slot number in the page
    } RID;

    // Attribute
    typedef enum {
        TypeInt = 0, TypeReal, TypeVarChar
    } AttrType;

    typedef unsigned AttrLength;

    typedef struct Attribute {
        std::string name;  // attribute name
        AttrType type;     // attribute type
        AttrLength length; // attribute length
    } Attribute;

    // Comparison Operator (NOT needed for part 1 of the project)
    typedef enum {
        EQ_OP = 0, // no condition// =
        LT_OP,      // <
        LE_OP,      // <=
        GT_OP,      // >
        GE_OP,      // >=
        NE_OP,      // !=
        NO_OP       // no condition
    } CompOp;


    /********************************************************************
    * The scan iterator is NOT required to be implemented for Project 1 *
    ********************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

    //  RBFM_ScanIterator is an iterator to go through records
    //  The way to use it is like the following:
    //  RBFM_ScanIterator rbfmScanIterator;
    //  rbfm.open(..., rbfmScanIterator);
    //  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
    //    process the data;
    //  }
    //  rbfmScanIterator.close();

    class RBFM_ScanIterator {
    public:
        RBFM_ScanIterator() = default;;

        ~RBFM_ScanIterator() = default;;

        // Never keep the results in the memory. When getNextRecord() is called,
        // a satisfying record needs to be fetched from the file.
        // "data" follows the same format as RecordBasedFileManager::insertRecord().
        RC getNextRecord(RID &rid, void* &data);

        RC close();
        RC setScanner(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const std::string &conditionAttribute
                , const CompOp compOp, const void* &value, std::vector<std::string> attributeNames);
    private:
        int currPageIndex;
        int currSlotNum;
        char* page;
        FileHandle fileHandle;
        std::vector<Attribute> recordDescriptor;
        std::string conditionAttribute;
        CompOp compOp;
        std::vector<std::string> attributeNames;
        const void *value;
        bool set_success = false;

        bool is_record_satisfiable(const RID &rid);
        RC create_data_with_required_attributes(const RID &rid, void *&data, bool is_internal);
        int get_sizeof_required_attributes_data(const RID &rid, void *&bitmap);
        AttrType get_attribute_type(std::string attributeName);
        int size_with_required_attributes(const RID &rid);
    };

    class RecordBasedFileManager {
    public:
        static RecordBasedFileManager &instance();                          // Access to the singleton instance

        RC createFile(const std::string &fileName);                         // Create a new record-based file

        RC destroyFile(const std::string &fileName);                        // Destroy a record-based file

        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a record-based file

        RC closeFile(FileHandle &fileHandle);                               // Close a record-based file

        //  Format of the data passed into the function is the following:
        //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
        //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
        //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
        //     Each bit represents whether each field value is null or not.
        //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
        //     If k-th bit from the left is set to 0, k-th field contains non-null values.
        //     If there are more than 8 fields, then you need to find the corresponding byte first,
        //     then find a corresponding bit inside that byte.
        //  2) Actual data is a concatenation of values of the attributes.
        //  3) For Int and Real: use 4 bytes to store the value;
        //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
        //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
        // For example, refer to the Q8 of Project 1 wiki page.

        // Insert a record into a file
        RC insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        RID &rid);

        // Read a record identified by the given rid.
        RC readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *&data);

        // Print the record that is passed to this utility method.
        // This method will be mainly used for debugging/testing.
        // The format is as follows:
        // field1-name: field1-value  field2-name: field2-value ... \n
        // (e.g., age: 24  height: 6.1  salary: 9000
        //        age: NULL  height: 7.5  salary: 7500)
        RC printRecord(const std::vector<Attribute> &recordDescriptor, const void *data, std::ostream &out);

        /*****************************************************************************************************
        * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) *
        * are NOT required to be implemented for Project 1                                                   *
        *****************************************************************************************************/
        // Delete a record identified by the given rid.
        RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

        // Assume the RID does not change after an update
        RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        const RID &rid);

        // Read an attribute given its name and the rid.
        RC readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                         const std::string &attributeName, void *&data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        RC scan(FileHandle &fileHandle,
                const std::vector<Attribute> &recordDescriptor,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RBFM_ScanIterator &rbfm_ScanIterator);
        bool is_slot_a_tombstone(int slotnum, void* page);
        RC readAttributeFromRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                   const RID &rid, const std::string &attributeName, void *&data, void *data_record);
        bool isColValueNull(const void *data, int k);
        bool is_slot_empty(char* page, int slotNum);
        bool is_slot_internal_id(char* page, int slotNum);
        RC internalRecordExtractor(RID &real_rid, int addr, int len, char* page, char* &record);
        RC originalRidExtractor_fromInternalId( char* page, RID given_rid, RID &original_rid);
    private:

        int calculateRecordSize(int N, const std::vector<Attribute> &recordDescriptor, const void *data, const std::vector<bool> &nullIndicator);
        void* encoder(const std::vector<Attribute> &recordDescriptor, const void *data, int& getRecordSize);
        RC copyInputToRecord(void* record, const void *data, const std::vector<Attribute> &recordDescriptor, const std::vector<bool> &nullIndicator, int N);
        int calculateDataSize(const std::vector<Attribute> &recordDescriptor, void* record, const std::vector<bool> &nullIndicator);
        RC copyRecordToData(const std::vector<Attribute> &recordDescriptor, void* data, void* record, const std::vector<bool> &nullIndicator);
        void* decoder(const std::vector<Attribute> &recordDescriptor, void* record, void *&data);
        int insertDataNewPage(FileHandle &fileHandle, int recordSize, void* record);
        int findFreePageIndex(FileHandle &fileHandle, int recordSize);
        int insertDataByPageIndex(FileHandle &fileHandle, int pageIndex, void* record, int recordSize);
        int getStartAddressOffset(int slotNum);
        int getLenAddressOffset(int slotNum);
        int free_slot_num(char* page, int numSlots);
        RC shiftRecordsLeft(char* page, int totalSlots, int startSlot, int shiftBy);
        RC shiftRecordsRight(char* page, int totalSlots, int startSlot, int shiftBy);
        RC insert_data_in_hole(char* page, void* record, int recordSize, int holeNum, int totalSlots);
        bool isTombStone(int address);
        bool isInternalId(int address);
        RC tombStonePointerExtractor(RID &rid, int addr, char* page);
        //RC internalRecordExtractor(RID &real_rid, int addr, int len, char* page, char* &record);
        RC insertMiscData(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,void *record, int recordSize, RID &rid);
        RC updateMiscRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, void *record, int updatedRecordSize, const RID &rid);
        RC deleteGivenRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);
//        RC calculateSizeofDataByRid(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
//                                                            const RID &rid, int &sizeOfData);

    protected:
        RecordBasedFileManager();                                                   // Prevent construction
        ~RecordBasedFileManager();                                                  // Prevent unwanted destruction
        RecordBasedFileManager(const RecordBasedFileManager &);                     // Prevent construction by copying
        RecordBasedFileManager &operator=(const RecordBasedFileManager &);          // Prevent assignment
    };

} // namespace PeterDB

#endif // _rbfm_h_
