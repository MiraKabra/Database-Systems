#include "src/include/rm.h"

namespace PeterDB {
    int RelationManager::tableCount;
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
//        std::string table = "Tables";
//        std::string column = "Columns";
//        std::string table_file = "Tables";
//        std::string column_file = "Columns";
        RelationManager::tableCount = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();

        //Create the table file , return -1 if it already exists
        if(rbfm.createFile(table_file) != 0) return -1;

        //Create the column file , return -1 if it already exists
        if(rbfm.createFile(column_file) != 0) return -1;

        rbfm.openFile(table_file, table_handle);
        rbfm.openFile(column_file, column_handle);
        //Create entries for 'Tables' table
        void* table_entry[2];

        createDataForTables_table(1, "Tables", 1, (char*)table_entry[0]);
        createDataForTables_table(2, "Columns", 1, (char*)table_entry[1]);

        //Create entries for 'Columns' table
        void* column_entry[9];

        createDataForColumns_table(1, "table-id", TypeInt, 4, 1, (char *)column_entry[0]);
        createDataForColumns_table(1, "table-name", TypeVarChar, 50, 2, (char *)column_entry[1]);
        createDataForColumns_table(1, "file-name", TypeVarChar, 50, 3, (char *)column_entry[2]);
        createDataForColumns_table(1, "system", TypeInt, 4, 4, (char *)column_entry[3]);
        createDataForColumns_table(2, "table-id", TypeInt, 4, 1, (char *)column_entry[4]);
        createDataForColumns_table(2, "column-name", TypeVarChar, 50, 2, (char *)column_entry[5]);
        createDataForColumns_table(2, "column-type", TypeInt, 4, 3, (char *)column_entry[6]);
        createDataForColumns_table(2, "column-length", TypeInt, 4, 4, (char *)column_entry[7]);
        createDataForColumns_table(2, "column-position", TypeInt, 4, 5, (char *)column_entry[8]);

        RID rid;
        //Enter the "Tables" and "Columns" entries
        for(int i = 0; i < 2; i++){
            rbfm.insertRecord(table_handle, getTableAttribute(), table_entry[i], rid);
            free(table_entry[i]);
        }

        for(int i = 0; i < 9; i++){
            rbfm.insertRecord(column_handle, getColumnAttribute(), column_entry[i], rid);
            free(column_entry[i]);
        }
        RelationManager::tableCount+=2;
        return 0;
    }

    std::vector<Attribute> RelationManager::getTableAttribute(){
        std::vector<Attribute> vector;
        Attribute attr;
        attr.name = "table-id";
        attr.length = 4;
        attr.type = TypeInt;
        vector.push_back(attr);

        attr.name = "table-name";
        attr.length = 50;
        attr.type = TypeVarChar;
        vector.push_back(attr);

        attr.name = "file-name";
        attr.length = 50;
        attr.type = TypeVarChar;
        vector.push_back(attr);

        attr.name = "system";
        attr.length = 4;
        attr.type = TypeInt;
        vector.push_back(attr);
        return vector;
    }

    std::vector<Attribute> RelationManager::getColumnAttribute(){
        std::vector<Attribute> vector;
        Attribute attr;
        attr.name = "table-id";
        attr.length = 4;
        attr.type = TypeInt;
        vector.push_back(attr);

        attr.name = "column-name";
        attr.length = 50;
        attr.type = TypeVarChar;
        vector.push_back(attr);

        attr.name = "column-type";
        attr.length = 4;
        attr.type = TypeInt;
        vector.push_back(attr);

        attr.name = "column-length";
        attr.length = 4;
        attr.type = TypeInt;
        vector.push_back(attr);

        attr.name = "column-position";
        attr.length = 4;
        attr.type = TypeInt;
        vector.push_back(attr);

        return vector;
    }

    /*
     * The columns are: table-id:int, table-name:varchar(50), file-name:varchar(50), system:int
     * table-name and file-name are same
     * The system field is used to represent boolean. 1 means the table is
     * system table and user can only do read operation here, no modification
     * The first entry will be: (1, "Tables", "Tables", 1)
     * The second entry will be: (2, "Columns", "Columns", 1)
     * */
    RC RelationManager::createDataForTables_table(int table_id, std::string table_name, int system, char* data){
        // 1 byte for bitmap (11110000)
        // 2*4 bytes for storing length(n) of varchar
        // 2*n bytes for storing the varchar
        //2*4 bytes for storing two ints
        int len = table_name.length();
        char bitmap = char(240); //corresponds to 11110000 bitmap
        const char* table_name_cstr = table_name.c_str();
        data = (char *)malloc(sizeof (char) + 2*sizeof (unsigned ) + 2*len + 2*sizeof (unsigned ));
        int offset = 0;
        memcpy(data+offset, &bitmap, sizeof (char));
        //Increase offset by 1
        offset += sizeof (char);
        //Store table-id
        memcpy(data+offset, &table_id, sizeof (unsigned ));
        //Increase offset by 4
        offset += sizeof (unsigned );
        //Store varchar length in next 4 byte
        memcpy(data + offset, &len, sizeof (unsigned ));
        //Increase offset by 4
        offset += sizeof (unsigned );
        //Store table-name in next 'len' bytes
        memcpy(data+offset, table_name_cstr, len);
        //Increase offset by 'len'
        offset += len;
        //Store varchar length in next 4 byte
        memcpy(data + offset, &len, sizeof (unsigned ));
        //Increase offset by 4
        offset += sizeof (unsigned );
        //Store file-name in next 'len' bytes
        memcpy(data+offset, table_name_cstr, len);
        //Increase offset by 'len'
        offset += len;
        //Store system value (bool : 1 or 0) in next 4 bytes
        memcpy(data+offset, &system, sizeof (unsigned ));

        return 0;
    }
    /*The columns are: table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int
     * first entry: (1, "table-id", TypeInt, 4 , 1)
     * second entry: (1, "table-name", TypeVarChar, 50, 2)
     * third entry: (1, "file-name", TypeVarChar, 50, 3)
     * fourth entry: (2, "table-id", TypeInt, 4, 1)
     * fifth entry: (2, "column-name",  TypeVarChar, 50, 2)
(2, "column-type", TypeInt, 4, 3)
     * sixth entry: (2, "column-type", TypeInt, 4, 3)
     * seventh entry: (2, "column-length", TypeInt, 4, 4)
     * eighth entry: (2, "column-position", TypeInt, 4, 5)
     * */
    RC RelationManager::createDataForColumns_table(int table_id, std::string column_name, int column_type, int column_length, int column_position, char* data){
        int len = column_name.length();
        char bitmap = char(240); //corresponds to 11110000 bitmap
        const char* column_name_cstr = column_name.c_str();
        data = (char*) malloc(sizeof (char) + sizeof (unsigned) + len + 4*sizeof (unsigned ));
        int offset = 0;
        memcpy(data+offset, &bitmap, sizeof (char ));
        offset += sizeof (char );
        memcpy(data+offset, &table_id, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy(data+offset, &len, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy(data+offset, column_name_cstr, len);
        offset += len;
        memcpy(data+offset, &column_type, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy(data+offset, &column_length, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy(data+offset, &column_position, sizeof (unsigned ));

        return 0;
    }

    RC RelationManager::deleteCatalog() {

        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        if(rbfm.destroyFile(table_file) != 0) return -1;
        if(rbfm.destroyFile(column_file) !=0) return -1;
        RelationManager::tableCount = 0;
        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        //Create a file for this table, return -1 if it already exists
        if(rbfm.createFile(tableName) != 0) return -1;
        RelationManager::tableCount++;
        RID rid;
        //Insert data in 'Tables' table
        void* data_for_tables_table;
        createDataForTables_table(RelationManager::tableCount, tableName, 0, (char*)data_for_tables_table);
        rbfm.insertRecord(table_handle, getTableAttribute(), data_for_tables_table, rid);
        free(data_for_tables_table);
        //Insert data in 'Columns' table
        for(int i = 0; i < attrs.size(); i++){
            void* data;
            Attribute attr = attrs.at(i);
            createDataForColumns_table(RelationManager::tableCount, attr.name, attr.type, attr.length, i+1, (char*)data);
            rbfm.insertRecord(column_handle, getColumnAttribute(), data, rid);
            free(data);
        }
        return 0;
    }


    RC RelationManager::deleteTable(const std::string &tableName) {

        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        RBFM_ScanIterator rbfm_ScanIterator;
        //Delete the file. Return -1 if error occurred
        if(rbfm.destroyFile(tableName) != 0) return -1;
        //Find the table id and rid for deleting this table
        std::vector<std::string> attributeNames_table;
        attributeNames_table.push_back("table-id");
        void* value;
        prepare_value_for_varchar(tableName, value);
        rbfm.scan(table_handle, getTableAttribute(), "table-name", EQ_OP, value, attributeNames_table, rbfm_ScanIterator);
        RID rid;
        void* data;
        bool found = false;
        int table_id;
        while (rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF){
            found = true;
            //In this data, there will be 1 byte bitmap followed by 4 bytes containing the 'table-id'(int)
            table_id = *(int*)((char*)data + sizeof (unsigned ));
            break;
        }
        if(!found){
            //No table entry was found in the catalog
            return -1;
        }
        //Found the table-id
        //Delete this record from 'tables' table
        rbfm.deleteRecord(table_handle, getTableAttribute(), rid);
        rbfm_ScanIterator.close();
        free(value);
        //Now find the entries with this table-id in 'columns' table one by one and delete them
        std::vector<std::string> attributeNames_column;
        attributeNames_column.push_back("table-id");
        rbfm.scan(column_handle, getColumnAttribute(), "table-id", EQ_OP, &table_id, attributeNames_column, rbfm_ScanIterator);
        found = false;
        while (rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF){
            found = true;
            rbfm.deleteRecord(column_handle, getColumnAttribute(), rid);
        }
        rbfm_ScanIterator.close();
        free(data);

        return 0;
    }
    RC RelationManager::prepare_value_for_varchar(const std::string &str, void* value){
        int len = str.length();
        const char* str_cstr = str.c_str();
        value = malloc(sizeof (unsigned ) + len);
        int offset = 0;
        memcpy((char*)value + offset, &len, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy((char*)value + offset, str_cstr, len);
        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        return -1;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        return -1;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        return -1;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        return -1;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        return -1;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return -1;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        return -1;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {

        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();


        return 0;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {

        return RM_EOF;
    }

    RC RM_ScanIterator::close() { return -1; }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

    // QE IX related
    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator){
        return -1;
    }


    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
        return -1;
    }

    RC RM_IndexScanIterator::close(){
        return -1;
    }

} // namespace PeterDB