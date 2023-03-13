#include "src/include/qe.h"
#include <cstring>
#include <algorithm>
#include <cmath>
#include <bitset>
namespace PeterDB {
    Filter::Filter(Iterator *input, const Condition &condition) {
        this->itr = input;
        this->cond = condition;
        itr->getAttributes(attributes);
    }

    Filter::~Filter() {

    }

    RC Filter::getNextTuple(void *data) {
        while(true){
            if(itr->getNextTuple(data)) return -1;
            if(is_record_satisfiable(data, cond)){
                return 0;
            }
        }
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        attrs = this->attributes;
        return 0;
    }

    bool Filter::is_record_satisfiable(void* data, Condition& cond){
        if(cond.op == NO_OP) return true;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();


        void* read_attr = nullptr;
        FileHandle dummy_fileHandle;
        RID dummy_rid;
        rbfm.readAttributeFromRecord(dummy_fileHandle, this->attributes, dummy_rid, cond.lhsAttr, read_attr, data);

        if(*(unsigned char *)read_attr == 128u) {
            free(read_attr);
            return false;
        }
        // find attr in recordDescriptor with name = conditionAttribute
        auto attr = find_if(
                this->attributes.begin(), this->attributes.end(),
                [&](const Attribute& attr) {return attr.name == cond.lhsAttr;}
        );
        if (attr == this->attributes.end()) {
            if(read_attr != nullptr){
                free(read_attr);
            }
            return false;
        }
        void *attr_val = (char*)read_attr + sizeof (char);
        if(attr->type == TypeInt || attr->type == TypeReal){
            int read_val = *(int *)((char*)attr_val);
            int given_val = *(int *)(cond.rhsValue.data);
            free(read_attr);
            switch(cond.op){
                case EQ_OP: return (read_val == given_val);
                case LT_OP: return (read_val < given_val);
                case LE_OP: return (read_val <= given_val);
                case GT_OP: return (read_val > given_val);
                case GE_OP: return (read_val >= given_val);
                case NE_OP: return (read_val != given_val);
            }
        }else if(attr->type == TypeReal){
            float read_val = *(float *)((char*)attr_val); free(read_attr);
            float given_val = *(float *)(cond.rhsValue.data);
            switch(cond.op){
                case EQ_OP: return (read_val == given_val);
                case LT_OP: return (read_val < given_val);
                case LE_OP: return (read_val <= given_val);
                case GT_OP: return (read_val > given_val);
                case GE_OP: return (read_val >= given_val);
                case NE_OP: return (read_val != given_val);
            }
        }else if(attr->type == TypeVarChar){
            int read_val_len = *(int*)((char*)attr_val);
            int given_val_len = *(int*)(cond.rhsValue.data);
            char read_val[read_val_len + 1];
            char given_val[given_val_len + 1];

            memcpy(&read_val, (char*)attr_val + sizeof (unsigned ), read_val_len);
            read_val[read_val_len] = '\0';
            free(read_attr);
            memcpy(&given_val, (char*)cond.rhsValue.data + sizeof (unsigned ), given_val_len);
            given_val[given_val_len] = '\0';
            switch(cond.op){
                case EQ_OP: return (strcmp(read_val, given_val) == 0);
                case LT_OP: return (strcmp(read_val, given_val) < 0);
                case LE_OP: return (strcmp(read_val, given_val) <= 0);
                case GT_OP: return (strcmp(read_val, given_val) > 0);
                case GE_OP: return (strcmp(read_val, given_val) >= 0);
                case NE_OP: return (strcmp(read_val, given_val) != 0);
            }
        }
        return false;
    }

    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
        this->itr = input;
        input->getAttributes(this->record_descriptor);
        this->relative_attrNames = attrNames;
    }

    Project::~Project() {

    }

    RC Project::getNextTuple(void *data) {
        while(true){
            void* total_data = malloc(PAGE_SIZE);
            memset(total_data, 0, PAGE_SIZE);
            if(itr->getNextTuple(total_data)){
                free(total_data);
                return -1;
            }
            create_data_with_required_attributes(total_data, data);
            free(total_data);
            return 0;
        }
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        if(attrs.size() != 0){
            attrs.clear();
        }
        for(std::string attr : this->relative_attrNames){
            for(int j = 0; j < this->record_descriptor.size(); j++){
                if(record_descriptor.at(j).name == attr){
                    attrs.push_back(record_descriptor.at(j));
                    break;
                }
            }
        }
        return 0;
    }

    RC Project::create_data_with_required_attributes(void* &total_data, void* &data){
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        auto numberOfCols = relative_attrNames.size();
        int bitMapSize = ceil((float)numberOfCols/8);
        void* bitmap = nullptr;

        set_bitmap(total_data, bitmap);

        int offset = 0;
        //copy bitmap
        memcpy((char*)data + offset, bitmap, bitMapSize);
        free(bitmap);
        offset += bitMapSize;

        for(int k = 0; k < relative_attrNames.size(); k++){
            std::string attributeName = relative_attrNames.at(k);
            AttrType type = get_attribute_type(attributeName);
            void* read_attr = nullptr;
            FileHandle dummy_fileHandle;
            RID dummy_rid;
            int rc = rbfm.readAttributeFromRecord(dummy_fileHandle, record_descriptor, dummy_rid, attributeName,read_attr, total_data);

            char* attr_data = (char*)read_attr + sizeof (char);
            if(*(unsigned char*)(read_attr) == 128u){
                free(read_attr);
                continue;
            }
            if (type == TypeInt || type == TypeReal){
                memcpy((char*)data + offset, attr_data, sizeof (unsigned ));
                offset += sizeof (unsigned );
            } else {
                int len = *(int*)(attr_data);
                memcpy((char*)data + offset, attr_data, sizeof (unsigned ) + len);
                offset += sizeof (unsigned ) + len;
            }
            free(read_attr);
        }
        return 0;
    }

    RC Project::set_bitmap(void* &total_data, void* &bitmap){

        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        int totalSize = 0;
        auto numberOfCols = this->relative_attrNames.size();
        const int bitMapSize = ceil((float)numberOfCols/8);
        totalSize += bitMapSize;

        std::vector<std::bitset<8>> bitMap(bitMapSize);
        if (bitmap == nullptr) {
            bitmap = malloc(bitMapSize * sizeof(char));
        }
        memset(bitmap, 0, bitMapSize*sizeof (char));

        for(int i = 0; i < relative_attrNames.size(); i++){
            std::string attributeName = relative_attrNames.at(i);
            AttrType type = get_attribute_type(attributeName);
            void* read_attr = nullptr;
            FileHandle dummy_fileHandle;
            RID dummy_rid;
            rbfm.readAttributeFromRecord(dummy_fileHandle, record_descriptor, dummy_rid, attributeName,
                                                  read_attr, total_data);
            if(*(unsigned char*)(read_attr) == 128u){
                bitMap[i/8].set(8 - i%8 - 1);
                free(read_attr);
                continue;
            }
            if(type == TypeInt || type == TypeReal){
                totalSize += sizeof (unsigned );
            } else {
                int len = *(int*)((char*)read_attr + sizeof (char ));
                totalSize += sizeof (unsigned ) + len;
            }
            free(read_attr);
        }

        memset(bitmap, 0, bitMapSize*sizeof (char));
        for(int i = 0; i < bitMapSize; i++) {
            char* pointer = (char*)&bitMap[i];
            memcpy((char*)bitmap + i, pointer, sizeof (char));
        }
        return totalSize;
    }

    AttrType Project::get_attribute_type(std::string attributeName){

        for(int k = 0; k < record_descriptor.size(); k++){
            Attribute attr = record_descriptor.at(k);
            if(attr.name == attributeName) return attr.type;
        }
        //random return just because we should return an Attrtype
        return TypeInt;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {

    }

    BNLJoin::~BNLJoin() {

    }

    RC BNLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {

    }

    INLJoin::~INLJoin() {

    }

    RC INLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {

    }

    GHJoin::~GHJoin() {

    }

    RC GHJoin::getNextTuple(void *data) {
        return -1;
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {

    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {

    }

    Aggregate::~Aggregate() {

    }

    RC Aggregate::getNextTuple(void *data) {
        return -1;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }
} // namespace PeterDB
