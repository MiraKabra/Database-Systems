#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include "src/include/rbfm.h"

namespace PeterDB {
#define RM_EOF (-1)  // end of a scan operator

    // RM_ScanIterator is an iterator to go through tuples
    class RM_ScanIterator {
    public:
        RM_ScanIterator();

        ~RM_ScanIterator();

        // "data" follows the same format as RelationManager::insertTuple()
        RC getNextTuple(RID &rid, void *data);

        RC close();
        RC setScanner(FileHandle &fileHandle,
                      const std::vector<Attribute> &recordDescriptor,
                      const std::string &conditionAttribute,
                      const CompOp compOp,
                      const void *value,
                      const std::vector<std::string> &attributeNames);
    private:
        RBFM_ScanIterator rbfm_ScanIterator;
        FileHandle fileHandle;
        bool set_success = false;
    };

    // RM_IndexScanIterator is an iterator to go through index entries
    class RM_IndexScanIterator {
    public:
        RM_IndexScanIterator();    // Constructor
        ~RM_IndexScanIterator();    // Destructor

        // "key" follows the same format as in IndexManager::insertEntry()
        RC getNextEntry(RID &rid, void *key);    // Get next matching entry
        RC close();                              // Terminate index scan

    };

    // Relation Manager
    class RelationManager {
    public:
        static int tableCount;
        std::string table = "Tables";
        std::string column = "Columns";
        std::string index = "Indexes";
        std::string table_file = "Tables";
        std::string column_file = "Columns";
        std::string index_file = "Indexes";
        FileHandle table_handle;
        FileHandle column_handle;
        FileHandle index_handle;
        bool catalog_exists = false;
        static RelationManager &instance();

        RC createCatalog();

        RC deleteCatalog();

        RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

        RC deleteTable(const std::string &tableName);

        RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

        RC insertTuple(const std::string &tableName, const void *data, RID &rid);

        RC deleteTuple(const std::string &tableName, const RID &rid);

        RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

        RC readTuple(const std::string &tableName, const RID &rid, void *data);

        // Print a tuple that is passed to this utility method.
        // The format is the same as printRecord().
        RC printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out);

        RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        // Do not store entire results in the scan iterator.
        RC scan(const std::string &tableName,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RM_ScanIterator &rm_ScanIterator);

        // Extra credit work (10 points)
        RC addAttribute(const std::string &tableName, const Attribute &attr);

        RC dropAttribute(const std::string &tableName, const std::string &attributeName);

        // QE IX related
        RC createIndex(const std::string &tableName, const std::string &attributeName);

        RC destroyIndex(const std::string &tableName, const std::string &attributeName);

        // indexScan returns an iterator to allow the caller to go through qualified entries in index
        RC indexScan(const std::string &tableName,
                     const std::string &attributeName,
                     const void *lowKey,
                     const void *highKey,
                     bool lowKeyInclusive,
                     bool highKeyInclusive,
                     RM_IndexScanIterator &rm_IndexScanIterator);
        bool fileExists(const std::string &fileName);

    private:
        RC createDataForTables_table(int table_id, std::string table_name,  int system, void* &data);
        RC createDataForColumns_table(int table_id, std::string column_name, int column_type, int column_length, int column_position, void* &data);
        std::vector<Attribute> getTableAttribute();
        std::vector<Attribute> getColumnAttribute();
        RC prepare_value_for_varchar(const std::string &str, void* &value);
        bool isSystemTable(const std::string &tableName);
    protected:
        RelationManager();                                                  // Prevent construction
        ~RelationManager();                                                 // Prevent unwanted destruction
        RelationManager(const RelationManager &);                           // Prevent construction by copying
        RelationManager &operator=(const RelationManager &);                // Prevent assignment

    };

} // namespace PeterDB

#endif // _rm_h_