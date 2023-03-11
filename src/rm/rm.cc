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
            if(!fileExists(this->index_file)){
                rbfm.createFile(this->index_file);
            }
            rbfm.openFile(this->index_file, this->index_handle);
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

        if(fileExists(this->index_file)){
            if(rbfm.destroyFile(this->index_file)) return -1;
        }
        if(rbfm.createFile(this->index_file)) return -1;
        if(rbfm.openFile(this->table_file, this->table_handle)) return -1;
        if(rbfm.openFile(this->column_file, this->column_handle)) return -1;
        if(rbfm.openFile(this->index_file, this->index_handle)) return -1;
        //Create entries for 'Tables' table
        void* table_entry[3] = {nullptr, nullptr, nullptr};

        createDataForTables_table(1, "Tables", 1, table_entry[0]);
        createDataForTables_table(2, "Columns", 1, table_entry[1]);
        createDataForTables_table(3, "Indexes", 1, table_entry[2]);
        //Create entries for 'Columns' table
        void* column_entry[12];
        for(int j = 0; j < 12; j++){
            column_entry[j] = nullptr;
        }

        createDataForColumns_table(1, "table-id", TypeInt, 4, 1, column_entry[0]);
        createDataForColumns_table(1, "table-name", TypeVarChar, 50, 2, column_entry[1]);
        createDataForColumns_table(1, "file-name", TypeVarChar, 50, 3, column_entry[2]);
        createDataForColumns_table(1, "system", TypeInt, 4, 4, column_entry[3]);
        createDataForColumns_table(2, "table-id", TypeInt, 4, 1, column_entry[4]);
        createDataForColumns_table(2, "column-name", TypeVarChar, 50, 2, column_entry[5]);
        createDataForColumns_table(2, "column-type", TypeInt, 4, 3, column_entry[6]);
        createDataForColumns_table(2, "column-length", TypeInt, 4, 4, column_entry[7]);
        createDataForColumns_table(2, "column-position", TypeInt, 4, 5, column_entry[8]);
        createDataForColumns_table(3, "table-id", TypeInt, 4, 1, column_entry[9]);
        createDataForColumns_table(3, "attribute-name", TypeVarChar, 50, 2, column_entry[10]);
        createDataForColumns_table(3, "file-name", TypeVarChar, 50, 3, column_entry[11]);

        RID rid;
        //Enter the "Tables" and "Columns" entries
        for(int i = 0; i < 3; i++){
            if(rbfm.insertRecord(table_handle, getTableAttribute(), table_entry[i], rid)){
                free(table_entry[i]);
                return -1;
            }
            free(table_entry[i]);
        }

        for(int i = 0; i < 12; i++) {
            if(i == 3) continue;
            if(rbfm.insertRecord(column_handle, getColumnAttribute(), column_entry[i], rid)){
                free(column_entry[i]);
                return -1;
            }
            free(column_entry[i]);
        }
        //As it is not freed in the loop above
        if(column_entry[3] != nullptr){
            free(column_entry[3]);
        }

        RelationManager::tableCount += 3;
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

        return v;
    }

    std::vector<Attribute> RelationManager::getIndexAttribute(){
        std::vector<Attribute> v;
        v.push_back({"table-id", TypeInt, 4});
        v.push_back({"attribute-name", TypeVarChar, 50});
        v.push_back({"file-name", TypeVarChar, 50});
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

    RC RelationManager::createDataForIndex_table(int table_id, std::string attributeName, std::string index_filename, void* &data){

        unsigned char bitmap = 0; // corresponds to 00000000 bitmap
        int attributeName_len = attributeName.length();
        int index_filename_len = index_filename.length();
        int total_size = sizeof (bitmap) + sizeof (table_id) + 2*sizeof (unsigned ) + attributeName_len + index_filename_len;
        const char* attributeName_cstr = attributeName.c_str();
        const char* index_filename_cstr = index_filename.c_str();

        data  = malloc(total_size);
        memset(data, 0, total_size);

        int offset = 0;

        memcpy((char*)data + offset, &bitmap, sizeof (char));
        offset += sizeof (char);

        //Store table_id
        memcpy((char*)data + offset, &table_id, sizeof (unsigned ));
        offset += sizeof (unsigned );

        //Store varchar length in next 4 byte
        memcpy((char*)data + offset, &attributeName_len, sizeof (unsigned ));
        offset += sizeof (unsigned );

        //Store attributeName in next 'attributeName_len' bytes
        memcpy((char*)data + offset, attributeName_cstr, attributeName_len);
        offset += attributeName_len;

        //Store varchar length in next 4 byte
        memcpy((char*)data + offset, &index_filename_len, sizeof (unsigned ));
        offset += sizeof (unsigned );

        memcpy((char*)data + offset, index_filename_cstr, index_filename_len);

        return 0;
    }
    //Here other than deleting the Indexes file, we also have to delete all the filename-attribute files that were created for every index.
    RC RelationManager::deleteCatalog() {

        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        if(rbfm.destroyFile(table_file) != 0) return -1;
        if(rbfm.destroyFile(column_file) != 0) return -1;
        if(fileExists(this->index_file)){
            if(rbfm.destroyFile(this->index_file)) return -1;
            RelationManager::index_handle = FileHandle();
        }
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
        if(tableName == this->table_file || tableName == this -> column_file) return -1;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        //Create a file for this table, return -1 if it already exists
        if(rbfm.createFile(tableName) != 0) return -1;
        RelationManager::tableCount++;
        RID rid;
        //Insert data in 'Tables' table
        void* data_for_tables_table = nullptr;
        createDataForTables_table(RelationManager::tableCount, tableName, 0, data_for_tables_table);
        if(rbfm.insertRecord(table_handle, getTableAttribute(), data_for_tables_table, rid)){
            free(data_for_tables_table);
            return -1;
        }
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

    bool RelationManager::isSystemTable(const std::string &tableName){
        if(!fileExists(tableName)) return -1;
        if(!this->catalog_exists) return -1;

        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        RBFM_ScanIterator rbfm_ScanIterator;

        //Find the table id and rid for deleting this table
        const std::vector<std::string> attributeNames_table = {"table-id", "system"};
        //const std::vector<std::string> attributeNames_table = {"table-id"};
        void* value = nullptr;
        prepare_value_for_varchar(tableName, value);
        if(rbfm.scan(table_handle, getTableAttribute(), "table-name", EQ_OP, value, attributeNames_table, rbfm_ScanIterator)){
            if(value != nullptr){
                free(value);
            }
            return -1;
        }

        RID rid;

        void* data = nullptr;
        if(rbfm_ScanIterator.getNextRecord(rid, data) == RBFM_EOF){
            if(value != nullptr){
                free(value);
            }
            if(data != nullptr){
                free(data);
            }
            return -1;
        }
        //In this data, there will be 1 byte bitmap followed by 4 bytes containing the 'table-id'(int)
        int table_id = *(int*)((char*)data + sizeof (char));
        int sys = *(int*)((char*)data + sizeof (char) + sizeof (unsigned ));
        rbfm_ScanIterator.close();
        //Found the table-id
        //If this is system table return true;
        free(data);
        free(value);
        if(sys == 1){
            return true;
        }
        return false;
    }
    /*
     * When deleting table, all the corresponding index files should be deleted*/
    RC RelationManager::deleteTable(const std::string &tableName) {

        if(!this->catalog_exists) return -1;
        //if (!fileExists("Tables") && !fileExists("Columns")) return -1;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        RBFM_ScanIterator rbfm_ScanIterator;
        //Delete the file. Return -1 if error occurred

        //Find the table id and rid for deleting this table
        const std::vector<std::string> attributeNames_table = {"table-id", "system"};
        //const std::vector<std::string> attributeNames_table = {"table-id"};
        void* value = nullptr;
        prepare_value_for_varchar(tableName, value);
        if(rbfm.scan(table_handle, getTableAttribute(), "table-name", EQ_OP, value, attributeNames_table, rbfm_ScanIterator)){
            if(value != nullptr){
                free(value);
            }
            return -1;
        }

        RID rid;

        void* data = nullptr;
        if(rbfm_ScanIterator.getNextRecord(rid, data) == RBFM_EOF){
            if(value != nullptr){
                free(value);
            }
            if(data != nullptr){
                free(data);
            }
            rbfm_ScanIterator.close();
            return -1;
        }
        //In this data, there will be 1 byte bitmap followed by 4 bytes containing the 'table-id'(int)
        int table_id = *(int*)((char*)data + sizeof (char));
        int sys = *(int*)((char*)data + sizeof (char) + sizeof (unsigned ));
        //Found the table-id
        //If this is system table, return error
        free(data);
        if(sys == 1){
            if(value != nullptr){
                free(value);
            }
            rbfm_ScanIterator.close();
            return -1;
        }
        //else Delete this record from 'tables' table
        if(rbfm.deleteRecord(table_handle, getTableAttribute(), rid)){
            if(value != nullptr){
                free(value);
            }
            rbfm_ScanIterator.close();
            return -1;
        }
        rbfm_ScanIterator.close();
        free(value);
        //Now find the entries with this table-id in 'columns' table one by one and delete them
        std::vector<std::string> attributeNames_column;
        attributeNames_column.push_back("table-id");
        if(rbfm.scan(column_handle, getColumnAttribute(), "table-id", EQ_OP, &table_id, attributeNames_column, rbfm_ScanIterator)) return -1;
        bool found = false;
        data = nullptr;
        while (rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF){
            found = true;
            if(rbfm.deleteRecord(column_handle, getColumnAttribute(), rid)){
                if(data != nullptr){
                    free(data);
                }
                rbfm_ScanIterator.close();
                return -1;
            }

            free(data);
            data = nullptr;
        }
        if(data != nullptr){
            free(data);
        }
        if(rbfm.destroyFile(tableName) != 0){
            rbfm_ScanIterator.close();
            return -1;
        }
        rbfm_ScanIterator.close();
        if(deleteAllCorrespondingIndexFiles(tableName, table_id)) return -1;

        return 0;
    }
    RC RelationManager::deleteAllCorrespondingIndexFiles(const std::string &tableName, int table_id){
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        IndexManager& ix = IndexManager::instance();
        RBFM_ScanIterator rbfm_ScanIterator;

        //Get the attribute names for which index files are there from the 'Indexes' table and then delete one by one
        const std::vector<std::string> attributeNames_table = {"attribute-name"};

        if(rbfm.scan(this->index_handle, getIndexAttribute(), "table-id", EQ_OP, &table_id, attributeNames_table, rbfm_ScanIterator));
        RID rid;
        void* data = nullptr;
        while(rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF){
            int len = *(int*)((char*)data + sizeof (char));
            char attribute_name[len + 1];
            attribute_name[len] = '\0';
            memcpy(&attribute_name, (char*)data + sizeof (char) + sizeof (unsigned ), len);
            std::string index_filename = tableName + "-" + attribute_name + ".idx";
            //destroy the index file
            //remove this record from 'Indexes' table
            if(ix.destroyFile(index_filename)) return -1;
            if(rbfm.deleteRecord(this->index_handle, getIndexAttribute(), rid)) return -1;
            free(data);
        }
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
        free(data);
        //Now find the entries with this table-id in 'columns' table one by one and add them to the attrs vector
        std::vector<std::string> attributeNames_column;
        attributeNames_column.push_back("column-name");
        attributeNames_column.push_back("column-type");
        attributeNames_column.push_back("column-length");

        if(rbfm.scan(column_handle, getColumnAttribute(), "table-id", EQ_OP, &table_id, attributeNames_column, rbfm_ScanIterator)) return -1;

        data = nullptr;

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
        if(data != nullptr){
            free(data);
        }
        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        if(isSystemTable(tableName)) return -1;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        FileHandle curr_file_handle;
        //Return error if file does not exist
        if(rbfm.openFile(tableName, curr_file_handle) != 0) return -1;
        std::vector<Attribute> recordDescriptor;
        getAttributes(tableName, recordDescriptor);
        /*
         * Get the After getting the attributes, get the table-id, and create <attribute-name, filename> pair*/
        if(rbfm.insertRecord(curr_file_handle, recordDescriptor, data, rid)) return -1;
        /*
         * For each attribute, create key*. Insert key*, rid in the corresponding index file*/
        if(rbfm.closeFile(curr_file_handle)) return -1;
        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        if(isSystemTable(tableName)) return -1;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        FileHandle curr_file_handle;
        //Return error if file does not exist
        if(rbfm.openFile(tableName, curr_file_handle) != 0) return -1;

        std::vector<Attribute> recordDescriptor;
        if(getAttributes(tableName, recordDescriptor)) return -1;
        /*
         * Get the After getting the attributes, get the table-id, and create <attribute-name, filename> pair*/
        if(rbfm.deleteRecord(curr_file_handle, recordDescriptor, rid)) return -1;
        /*
         * For each attribute, create key*. Delete key*, rid in the corresponding index file*/
        if(rbfm.closeFile(curr_file_handle)) return -1;

        return 0;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        if(isSystemTable(tableName)) return -1;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        FileHandle curr_file_handle;
        //Return error if file does not exist
        if(rbfm.openFile(tableName, curr_file_handle) != 0) return -1;

        std::vector<Attribute> recordDescriptor;
        getAttributes(tableName, recordDescriptor);
        /*
         * Get the After getting the attributes, get the table-id, and create <attribute-name, filename> pair
         * Get the current data from given rid
         * Prepare keys from current data and delete the entries in index file*/
        if(rbfm.updateRecord(curr_file_handle, recordDescriptor, data, rid)) return -1;
        /*
         * Create keys from the updated data and insert key,rid pair in the index files*/
        if(rbfm.closeFile(curr_file_handle)) return -1;
        return 0;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        FileHandle curr_file_handle;
        //Return error if file does not exist
        if(rbfm.openFile(tableName, curr_file_handle) != 0) return -1;
        std::vector<Attribute> recordDescriptor;
        if(getAttributes(tableName, recordDescriptor)){
            if(rbfm.closeFile(curr_file_handle)) return -1;
            return -1;
        }

        if(rbfm.readRecord(curr_file_handle, recordDescriptor, rid, data)){
            if(rbfm.closeFile(curr_file_handle)) return -1;
            return -1;
        }
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
            if(k < numberOfCols - 1) out << ", ";
            else out << "\n";
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
        if(rm_ScanIterator.setScanner(curr_file_handle, recordDescriptor, conditionAttribute, compOp, value, attributeNames) != 0) return -1;
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
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        IndexManager& ix = IndexManager::instance();
        std::string index_filename = tableName + "-" + attributeName + ".idx";
        if(rbfm.createFile(index_filename) != 0) return -1;

        //Get table-id for this tableName
        RBFM_ScanIterator rbfm_ScanIterator;
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
        free(data);

        void* data_index = nullptr;
        createDataForIndex_table(table_id, attributeName, index_filename, data_index);

        if(rbfm.insertRecord(this->index_handle, getIndexAttribute(), data_index, rid)) return -1;

        //Insert all the entries for tuple in 'tableName'
        FileHandle curr_file_handle;
        if(rbfm.openFile(tableName, curr_file_handle)) return -1;
        std::vector<Attribute> recordDescriptor;
        if(getAttributes(tableName, recordDescriptor)){
            if(rbfm.closeFile(curr_file_handle)) return -1;
            return -1;
        }
        // We need to extract <key, rid> pair
        const std::vector<std::string> attributeNames = {attributeName};
        Attribute attribute;
        for(Attribute attr: recordDescriptor){
            if(strcmp(attr.name.c_str(), attributeName.c_str()) == 0){
                attribute = attr;
                break;
            }
        }
        if(rbfm.scan(curr_file_handle, recordDescriptor, "", NO_OP, nullptr, attributeNames, rbfm_ScanIterator))return -1;


        data = nullptr;
        IXFileHandle index_filename_handle;
        if(ix.openFile(index_filename, index_filename_handle)) return -1;
        while(rbfm_ScanIterator.getNextRecord(rid, data) != RBFM_EOF){
            //In this data, there will be 1 byte bitmap followed by key
            void* key = nullptr;
            if(attribute.type == TypeInt){
                key = malloc(sizeof (unsigned ));
                memcpy(key, (char*)data + sizeof (char ), sizeof (unsigned ));
            }else if(attribute.type = TypeReal){
                key = malloc(sizeof (float ));
                memcpy(key, (char*)data + sizeof (char ), sizeof (float ));
            }else{
                int len = *(int*)((char*)data + sizeof (char ));
                key = malloc(sizeof (unsigned ) + len);
                memcpy(key, (char*)data + sizeof (char), sizeof (unsigned ) + len);
            }
            ix.insertEntry(index_filename_handle, attribute, key, rid);
            free(key);
        }
        return 0;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName){
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        std::string index_filename = tableName + "-" + attributeName + ".idx";
        if(!fileExists(index_filename)) return -1;
        RBFM_ScanIterator rbfm_ScanIterator;
        const std::vector<std::string> attributeNames_table = {"table-id"};
        void* value = nullptr;
        prepare_value_for_varchar(attributeName, value);
        if(rbfm.scan(this->index_handle, getIndexAttribute(), "attribute-name", EQ_OP, value, attributeNames_table, rbfm_ScanIterator)){
            if(value != nullptr){
                free(value);
            }
            return -1;
        }
        RID rid;
        void* data = nullptr;
        if(rbfm_ScanIterator.getNextRecord(rid, data) == RBFM_EOF){
            if(value != nullptr){
                free(value);
            }
            if(data != nullptr){
                free(data);
            }
            rbfm_ScanIterator.close();
            return -1;
        }
        free(data);
        //We just needed the rid, to delete this record from "Indexes"
        if(rbfm.deleteRecord(this->index_handle, getIndexAttribute(), rid)){
            if(value != nullptr){
                free(value);
                rbfm_ScanIterator.close();
                return -1;
            }
        }
        rbfm_ScanIterator.close();
        free(value);
        //delete the file itself
        if(rbfm.destroyFile(index_filename)) return -1;
        return 0;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator){
        IndexManager& ix = IndexManager::instance();
        IXFileHandle curr_ix_file_handle;
        Attribute attribute = get_attribute_from_name(tableName, attributeName);
        if(ix.openFile(tableName, curr_ix_file_handle)) return -1;
        rm_IndexScanIterator.setScanner(curr_ix_file_handle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
        return 0;
    }

    Attribute RelationManager::get_attribute_from_name(const std::string &tableName, const std::string &attributeName){
        Attribute attr;
        std::vector<Attribute> recordDescriptor;
        getAttributes(tableName, recordDescriptor);
        for(Attribute attribute: recordDescriptor){
            if(attribute.name == attributeName){
                attr = attribute;
                break;
            }
        }
        return attr;
    }

    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
        if(this->ix_ScanIterator.getNextEntry(rid, key) == RM_EOF) return RM_EOF;
        return 0;
    }

    RC RM_IndexScanIterator::setScanner(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *lowKey, const void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
        IndexManager& ix = IndexManager::instance();
        this->ixFileHandle = ixFileHandle;
        int rc = ix.scan(this->ixFileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, this->ix_ScanIterator);
        if(rc == 0){
            this->set_success = true;
        }
        return rc;
    }

    RC RM_IndexScanIterator::close(){
        IndexManager& ix = IndexManager::instance();
        if(set_success){
            ix.closeFile(this->ixFileHandle);
            this->ix_ScanIterator.close();
        }
        this->set_success = false;
        return 0;
    }

} // namespace PeterDB