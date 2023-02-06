#include "src/include/rm.h"
#include <cstring>
#include <cmath>
#include <iostream>
#include <sys/stat.h>

namespace PeterDB {
    int RelationManager::tableCount;
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager(){
        RBFM_ScanIterator rbfm_ScanIterator;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        if(fileExists(this->table_file) && fileExists(this->column_file)){
            rbfm.openFile(this->table_file, this->table_handle);
            rbfm.openFile(this-> column_file, this->column_handle);

            int count = 0;
            std::vector<std::string> attributeNames;
            void* value = malloc(10);
            void* data = malloc(100);
            RID rid;
            rbfm.scan(table_handle, getTableAttribute(), "table-id", NO_OP, value, attributeNames, rbfm_ScanIterator);
            while(rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF){
                count++;
            }
            rbfm_ScanIterator.close();
            free(value);
            free(data);
            this->tableCount = count;
            this->catalog_exists = true;
        }
    }

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    bool RelationManager::fileExists(const std::string &fileName) {
        struct stat stFileInfo{};

        return stat(fileName.c_str(), &stFileInfo) == 0;
    }

    RC RelationManager::createCatalog() {
//        std::string table = "Tables";
//        std::string column = "Columns";
//        std::string table_file = "Tables";
//        std::string column_file = "Columns";
        RelationManager::tableCount = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();

        //Create the table file , return -1 if it already exists
        if(rbfm.createFile(this->table_file) != 0) return -1;
        //rbfm.createFile(table_file);
        //Create the column file , return -1 if it already exists
        if(rbfm.createFile(this->column_file) != 0) return -1;
        //rbfm.createFile(column_file);
        if(rbfm.openFile(this->table_file, this->table_handle)) return -1;
        if(rbfm.openFile(this->column_file, this->column_handle)) return -1;
        //Create entries for 'Tables' table
        void* table_entry[2];

        createDataForTables_table(1, "Tables", 1, table_entry[0]);
        createDataForTables_table(2, "Columns", 1, table_entry[1]);

        //Create entries for 'Columns' table
        void* column_entry[9];

        createDataForColumns_table(1, "table-id", TypeInt, 4, 1, column_entry[0]);
        createDataForColumns_table(1, "table-name", TypeVarChar, 50, 2, column_entry[1]);
        createDataForColumns_table(1, "file-name", TypeVarChar, 50, 3, column_entry[2]);
        createDataForColumns_table(1, "system", TypeInt, 4, 4, column_entry[3]);
        createDataForColumns_table(2, "table-id", TypeInt, 4, 1, column_entry[4]);
        createDataForColumns_table(2, "column-name", TypeVarChar, 50, 2, column_entry[5]);
        createDataForColumns_table(2, "column-type", TypeInt, 4, 3, column_entry[6]);
        createDataForColumns_table(2, "column-length", TypeInt, 4, 4, column_entry[7]);
        createDataForColumns_table(2, "column-position", TypeInt, 4, 5, column_entry[8]);

        RID rid;
        //Enter the "Tables" and "Columns" entries
        for(int i = 0; i < 2; i++){
            if(rbfm.insertRecord(table_handle, getTableAttribute(), table_entry[i], rid)) return -1;
            free(table_entry[i]);
        }

        for(int i = 0; i < 9; i++) {
            if(rbfm.insertRecord(column_handle, getColumnAttribute(), column_entry[i], rid)) return -1;
            free(column_entry[i]);
        }
        RelationManager::tableCount += 2;
        this->catalog_exists = true;
        return 0;
    }

    std::vector<Attribute> RelationManager::getTableAttribute(){
        std::vector<Attribute> v;
        v.push_back({"table-id", TypeInt, 4});
        v.push_back({"table-name", TypeVarChar, 50});
        v.push_back({"file-name", TypeVarChar, 50});
        v.push_back({"system", TypeInt, 4});

//        Attribute attr;
//        attr.name = "table-id";
//        attr.length = 4;
//        attr.type = TypeInt;
//        v.push_back(attr);
//
//        attr.name = "table-name";
//        attr.length = 50;
//        attr.type = TypeVarChar;
//        v.push_back(attr);
//
//        attr.name = "file-name";
//        attr.length = 50;
//        attr.type = TypeVarChar;
//        v.push_back(attr);
//
//        attr.name = "system";
//        attr.length = 4;
//        attr.type = TypeInt;
//        v.push_back(attr);
        return v;
    }

    std::vector<Attribute> RelationManager::getColumnAttribute(){
        std::vector<Attribute> v;
        v.push_back({"table-id", TypeInt, 4});
        v.push_back({"column-name", TypeVarChar, 50});
        v.push_back({"column-type", TypeInt, 4});
        v.push_back({"column-length", TypeInt, 4});
        v.push_back({"column-position", TypeInt, 4});

//        Attribute attr;
//        attr.name = "table-id";
//        attr.length = 4;
//        attr.type = TypeInt;
//        v.push_back(attr);
//
//        attr.name = "column-name";
//        attr.length = 50;
//        attr.type = TypeVarChar;
//        v.push_back(attr);
//
//        attr.name = "column-type";
//        attr.length = 4;
//        attr.type = TypeInt;
//        v.push_back(attr);
//
//        attr.name = "column-length";
//        attr.length = 4;
//        attr.type = TypeInt;
//        v.push_back(attr);
//
//        attr.name = "column-position";
//        attr.length = 4;
//        attr.type = TypeInt;
//        v.push_back(attr);

        return v;
    }

    /*
     * The columns are: table-id:int, table-name:varchar(50), file-name:varchar(50), system:int
     * table-name and file-name are same
     * The system field is used to represent boolean. 1 means the table is
     * system table and user can only do read operation here, no modification
     * The first entry will be: (1, "Tables", "Tables", 1)
     * The second entry will be: (2, "Columns", "Columns", 1)
     * */
    RC RelationManager::createDataForTables_table(int table_id, std::string table_name, int system, void* &data){
        // 1 byte for bitmap (11110000)
        // 2*4 bytes for storing length(n) of varchar
        // 2*n bytes for storing the varchar
        //2*4 bytes for storing two ints
        int len = table_name.length();
        unsigned char bitmap = 0; // corresponds to 00000000 bitmap
        const char* table_name_cstr = table_name.c_str();

        int total_size = sizeof (bitmap) + sizeof (table_id) + 2 * sizeof(len) + 2*len + sizeof (system);
        data = malloc(total_size); memset(data, 0, total_size);
//        memset(data, 0, sizeof (bitmap) + 2*sizeof (unsigned ) + 2*len + 2*sizeof (unsigned ));

        int offset = 0;
        memcpy((char*)data + offset, &bitmap, sizeof (char)); offset += sizeof (char);

        //Store table-id
        memcpy((char*)data + offset, &table_id, sizeof (unsigned )); offset += sizeof (unsigned );

        //Store varchar length in next 4 byte
        memcpy((char*)data + offset, &len, sizeof (unsigned )); offset += sizeof (unsigned );

        //Store table-name in next 'len' bytes
        memcpy((char*)data + offset, table_name_cstr, len); offset += len;

        //Store varchar length in next 4 byte
        memcpy((char*)data + offset, &len, sizeof (unsigned )); offset += sizeof (unsigned );

        //Store file-name in next 'len' bytes
        memcpy((char*)data + offset, table_name_cstr, len); offset += len;

        //Store system value (bool : 1 or 0) in next 4 bytes
        memcpy((char*)data+offset, &system, sizeof (unsigned ));

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
    RC RelationManager::createDataForColumns_table(int table_id, std::string column_name, int column_type, int column_length, int column_position, void* &data){
        int len = column_name.length();
        unsigned char bitmap = 0; //corresponds to 00000000 bitmap
        const char* column_name_cstr = column_name.c_str();
        data = malloc(sizeof (char) + sizeof (unsigned) + len + 4*sizeof (unsigned ));
        int offset = 0;
        memcpy((char*)data+offset, &bitmap, sizeof (char )); offset += sizeof (char );
        memcpy((char*)data+offset, &table_id, sizeof (unsigned )); offset += sizeof (unsigned );
        memcpy((char*)data+offset, &len, sizeof (unsigned )); offset += sizeof (unsigned );
        memcpy((char*)data+offset, column_name_cstr, len); offset += len;
        memcpy((char*)data+offset, &column_type, sizeof (unsigned )); offset += sizeof (unsigned );
        memcpy((char*)data+offset, &column_length, sizeof (unsigned )); offset += sizeof (unsigned );
        memcpy((char*)data+offset, &column_position, sizeof (unsigned ));

        return 0;
    }

    RC RelationManager::deleteCatalog() {

        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        if(rbfm.destroyFile(table_file) != 0) return -1;
        if(rbfm.destroyFile(column_file) != 0) return -1;
        RelationManager::table_handle = FileHandle();
        RelationManager::column_handle = FileHandle();
        RelationManager::tableCount = 0;
        this->catalog_exists = false;
        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        //should fail if no catalog was created before calling it
        if(!this->catalog_exists) return -1;
        //if (!fileExists("Tables") && !fileExists("Columns")) return -1;

        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        //Create a file for this table, return -1 if it already exists
        if(rbfm.createFile(tableName) != 0) return -1;
        RelationManager::tableCount++;
        RID rid;
        //Insert data in 'Tables' table
        void* data_for_tables_table = nullptr;
        createDataForTables_table(RelationManager::tableCount, tableName, 0, data_for_tables_table);
        if(rbfm.insertRecord(table_handle, getTableAttribute(), data_for_tables_table, rid)) return -1;
        free(data_for_tables_table);
        //Insert data in 'Columns' table
        for(int i = 0; i < attrs.size(); i++){
            void* data = nullptr;
            Attribute attr = attrs.at(i);
            createDataForColumns_table(RelationManager::tableCount, attr.name, attr.type, attr.length, i+1, data);
            if(rbfm.insertRecord(column_handle, getColumnAttribute(), data, rid)) return -1;
            free(data);
        }
        return 0;
    }


    RC RelationManager::deleteTable(const std::string &tableName) {

        if(!this->catalog_exists) return -1;
        //if (!fileExists("Tables") && !fileExists("Columns")) return -1;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        RBFM_ScanIterator rbfm_ScanIterator;
        //Delete the file. Return -1 if error occurred
        if(rbfm.destroyFile(tableName) != 0) return -1;
        //Find the table id and rid for deleting this table
        //const std::vector<std::string> attributeNames_table = {"table-id", "system"};
        const std::vector<std::string> attributeNames_table = {"table-id"};
        void* value;
        prepare_value_for_varchar(tableName, value);
        if(rbfm.scan(table_handle, getTableAttribute(), "table-name", EQ_OP, value, attributeNames_table, rbfm_ScanIterator)) return -1;

        RID rid;

        void* data = nullptr;
        if(rbfm_ScanIterator.getNextRecord(rid, data) == RBFM_EOF) return -1;
        //In this data, there will be 1 byte bitmap followed by 4 bytes containing the 'table-id'(int)
        int table_id = *(int*)((char*)data + sizeof (char));
        //int sys = *(int*)((char*)data + sizeof (char) + sizeof (unsigned ));
        //Found the table-id
        //If this is system table, return error
//        if(sys == 1){
//            rbfm_ScanIterator.close();
//            return -1;
//        }
        //else Delete this record from 'tables' table
        if(rbfm.deleteRecord(table_handle, getTableAttribute(), rid)) return -1;
        rbfm_ScanIterator.close();
        free(value);
        //Now find the entries with this table-id in 'columns' table one by one and delete them
        std::vector<std::string> attributeNames_column;
        attributeNames_column.push_back("table-id");
        if(rbfm.scan(column_handle, getColumnAttribute(), "table-id", EQ_OP, &table_id, attributeNames_column, rbfm_ScanIterator)) return -1;
        bool found = false;
        while (rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF){
            found = true;
            if(rbfm.deleteRecord(column_handle, getColumnAttribute(), rid)) return -1;
        }
        rbfm_ScanIterator.close();

        return 0;
    }
    RC RelationManager::prepare_value_for_varchar(const std::string &str, void* &value){
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

        if(!this->catalog_exists) return -1;
        //if (!fileExists("Tables") && !fileExists("Columns")) return -1;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        RBFM_ScanIterator rbfm_ScanIterator;
        //Find the table id for this table
        const std::vector<std::string> attributeNames_table = {"table-id"};
        void* value = nullptr;
        prepare_value_for_varchar(tableName, value);
        if(rbfm.scan(table_handle, getTableAttribute(), "table-name", EQ_OP, value, attributeNames_table, rbfm_ScanIterator)) return -1;

        RID rid;
        void* data = nullptr;

        if(rbfm_ScanIterator.getNextRecord(rid, data) == RBFM_EOF) return -1;

        //In this data, there will be 1 byte bitmap followed by 4 bytes containing the 'table-id'(int)
        int table_id = *(int*)((char*)data + sizeof (char));
        //Found the table-id
        rbfm_ScanIterator.close();
        free(value);

        //Now find the entries with this table-id in 'columns' table one by one and add them to the attrs vector
        std::vector<std::string> attributeNames_column;
        attributeNames_column.push_back("column-name");
        attributeNames_column.push_back("column-type");
        attributeNames_column.push_back("column-length");

        if(rbfm.scan(column_handle, getColumnAttribute(), "table-id", EQ_OP, &table_id, attributeNames_column, rbfm_ScanIterator)) return -1;


        while (rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF){
            int offset = sizeof(char);
            int col_name_len = *(int*)((char*) data + offset);
            offset += sizeof (unsigned );
            char array[col_name_len + 1];
            //const char* col_name_ptr = (char*)malloc(col_name_len);
            //memcpy((void*)col_name_ptr, (char*)data + offset, col_name_len);
            memcpy(array, (char*)data + offset, col_name_len);
            array[col_name_len] = '\0';
            //Store attribute name
            std::string column_name = array;
            //increase offset
            offset += col_name_len;
            AttrType column_type = (AttrType)(*(int*)((char*)data + offset));
            offset += sizeof (unsigned );
            AttrLength column_length = (AttrLength)(*(int*)((char*)data + offset));
            attrs.push_back({column_name, column_type, column_length});
            free(data);
            data = nullptr;
        }
        rbfm_ScanIterator.close();

        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        FileHandle curr_file_handle;
        //Return error if file does not exist
        if(rbfm.openFile(tableName, curr_file_handle) != 0) return -1;
        std::vector<Attribute> recordDescriptor;
        getAttributes(tableName, recordDescriptor);
        if(rbfm.insertRecord(curr_file_handle, recordDescriptor, data, rid)) return -1;
        if(rbfm.closeFile(curr_file_handle)) return -1;
        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {

        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        FileHandle curr_file_handle;
        //Return error if file does not exist
        if(rbfm.openFile(tableName, curr_file_handle) != 0) return -1;

        std::vector<Attribute> recordDescriptor;
        if(getAttributes(tableName, recordDescriptor)) return -1;
        if(rbfm.deleteRecord(curr_file_handle, recordDescriptor, rid)) return -1;
        if(rbfm.closeFile(curr_file_handle)) return -1;

        return 0;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        FileHandle curr_file_handle;
        //Return error if file does not exist
        if(rbfm.openFile(tableName, curr_file_handle) != 0) return -1;

        std::vector<Attribute> recordDescriptor;
        getAttributes(tableName, recordDescriptor);
        if(rbfm.updateRecord(curr_file_handle, recordDescriptor, data, rid)) return -1;
        if(rbfm.closeFile(curr_file_handle)) return -1;
        return 0;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        FileHandle curr_file_handle;
        //Return error if file does not exist
        if(rbfm.openFile(tableName, curr_file_handle) != 0) return -1;
        std::vector<Attribute> recordDescriptor;
        if(getAttributes(tableName, recordDescriptor)) return -1;

        if(rbfm.readRecord(curr_file_handle, recordDescriptor, rid, data)) return -1;
        if(rbfm.closeFile(curr_file_handle)) return -1;

        return 0;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        int numberOfCols = attrs.size();
        int bitMapSize = ceil((float)numberOfCols/8); // eg. 1 for 3 cols, 2 for 9 cols
        char* pointer = (char *)data;

        int N = 0;
        std::vector<bool> nullIndicator;

        for(int k = 0; k < numberOfCols; k++){
            bool isNull = rbfm.isColValueNull(data, k);
            nullIndicator.push_back(isNull);
            if(isNull) continue;
            N++; //Increase N for non null value
        }
        pointer = pointer + bitMapSize;
        int offset = bitMapSize;
//        std::string str = "";
        //<AttributeName1>:\s<Value1>,\s<AttributeName2>:\s<Value2>,\s<AttributeName3>:\s<Value3>\n
        for(int k = 0; k < numberOfCols; k++){
            Attribute attr = attrs.at(k);
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

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();

        FileHandle curr_file_handle;
        //Return error if file does not exist
        if(rbfm.openFile(tableName, curr_file_handle) != 0) return -1;

        std::vector<Attribute> recordDescriptor;
        getAttributes(tableName, recordDescriptor);

        if(rbfm.readAttribute(curr_file_handle, recordDescriptor, rid, attributeName, data)) return -1;
        if(rbfm.closeFile(curr_file_handle)) return -1;
        return 0;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {

        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        FileHandle curr_file_handle;
        //Return error if file does not exist
        if(rbfm.openFile(tableName, curr_file_handle) != 0) return -1;

        std::vector<Attribute> recordDescriptor;
        getAttributes(tableName, recordDescriptor);
        rm_ScanIterator.setScanner(curr_file_handle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
        return 0;
    }

    RC RM_ScanIterator::setScanner(FileHandle &fileHandle,
                                   const std::vector<Attribute> &recordDescriptor,
                                   const std::string &conditionAttribute,
                                   const CompOp compOp,
                                   const void *value,
                                   const std::vector<std::string> &attributeNames){

        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        this->fileHandle = fileHandle;
        int rc = rbfm.scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, this->rbfm_ScanIterator);
        if(rc == 0){
            this->set_success = true;
        }
        return rc;
    }
    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        //If the caller (eg. test) has allocated some memory to *data before
        //passing it to this function, then it will copy the result in
        //the correct place.
        //If I ever want to call this, I should either first set 'data = nullptr' or make data = malloc(some_size) [where size can be PAGE_SIZE]
        if(this->rbfm_ScanIterator.getNextRecord(rid, data) == RM_EOF) return RM_EOF;
        return 0;
    }

    RC RM_ScanIterator::close() {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        if(set_success){
            rbfm.closeFile(this->fileHandle);
            rbfm_ScanIterator.close();
        }
        this->set_success = false;
        return 0;
    }

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