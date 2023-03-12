#include "src/include/qe.h"
#include <cstring>
#include <algorithm>
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

    }

    Project::~Project() {

    }

    RC Project::getNextTuple(void *data) {
        return -1;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
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
