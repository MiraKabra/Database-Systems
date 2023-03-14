#include "src/include/qe.h"
#include <cstring>
#include <algorithm>
#include <cmath>
#include <bitset>
#include <unordered_map>

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
        this->bnl_leftIn = leftIn;
        this->bnl_rightIn = rightIn;
        this->bnl_righIn_initial = rightIn;
        this->bnl_condition = condition;
        this->bnl_numPages = numPages;
        this->start = true;
        this->curr_output_index = -1;
        this->finished_scan_left_table = false;
        this->finished_scan_right_table = false;
        leftIn->getAttributes(this->leftIn_attrs);
        rightIn->getAttributes(this->rightIn_attrs);
        this->getAttributes(joined_attrs);
        for(Attribute attribute : leftIn_attrs){
            if(attribute.name == condition.lhsAttr){
                this->join_keyType = attribute.type;
                break;
            }
        }
    }

    int BNLJoin::getSizeOfData(void* &data, std::vector<Attribute> &recordDescriptor){
        int result = 0;
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        int numberOfCols = recordDescriptor.size();
        int bitMapSize = ceil((float)numberOfCols/8);
        result += bitMapSize;

        int N = 0;
        std::vector<bool> nullIndicator;
        for(int k = 0; k < numberOfCols; k++){
            bool isNull = rbfm.isColValueNull(data, k);
            nullIndicator.push_back(isNull);
            N++;
        }
        char* pointer = (char *)data + bitMapSize * sizeof(char);
        for(int k = 0; k < numberOfCols; k++){
            bool isNull = nullIndicator.at(k);
            if(isNull) continue;
            Attribute attr = recordDescriptor.at(k);
            if(attr.type == TypeVarChar){
                unsigned len = *(int *) pointer;
                result += sizeof (unsigned ) + len;
                pointer = pointer + len + 4;
            } else if(attr.type == TypeInt){
                result += sizeof (unsigned );
                pointer += sizeof (unsigned );
            }else{
                result += sizeof (float );
                pointer += sizeof (float );
            }
        }
        return result;
    }

    BNLJoin::~BNLJoin() {

    }

    RC BNLJoin::getNextTuple(void *data) {

        if(this->join_keyType == TypeInt){
            if(start){
                start = false;
                loadTuplesLeftTable_TypeInt(this->left_map_int);
                loadTuplesRightTable_TypeInt(this->right_map_int);
                joinTwoTables_TypeInt(this->left_map_int, this->right_map_int, this->output);
            }
            curr_output_index++;
            //all exhausted, load again
            if(curr_output_index == output.size()){
                if(!finished_scan_right_table){

                    //free data in right map
                    for(auto const &right_pair: right_map_int){
                        for(void* right_data: right_pair.second){
                            free(right_data);
                        }
                    }
                    right_map_int.clear();

                    //free data in output
                    for(void* data: output){
                        free(data);
                    }
                    output.clear();

                    loadTuplesRightTable_TypeInt(this->right_map_int);
                    joinTwoTables_TypeInt(this->left_map_int, this->right_map_int, this->output);
                    curr_output_index = 0;
                }else if(!finished_scan_left_table){

                    //free data in left map
                    for(auto const &left_pair: left_map_int){
                        for(void* left_data: left_pair.second){
                            free(left_data);
                        }
                    }
                    left_map_int.clear();

                    //free data in right map
                    for(auto const &right_pair: right_map_int){
                        for(void* right_data: right_pair.second){
                            free(right_data);
                        }
                    }
                    right_map_int.clear();

                    //free data in output
                    for(void* data: output){
                        free(data);
                    }
                    output.clear();

                    this->finished_scan_right_table = false;

                    loadTuplesLeftTable_TypeInt(this->left_map_int);
                    //set iterator of right table to initial position again
                    this->bnl_rightIn = this->bnl_righIn_initial;
                    loadTuplesRightTable_TypeInt(this->right_map_int);
                    joinTwoTables_TypeInt(this->left_map_int, this->right_map_int, this->output);
                    curr_output_index = 0;
                }else{
                    //Both left and right scan are complete
                    //free data in left map
                    for(auto const &left_pair: left_map_int){
                        for(void* left_data: left_pair.second){
                            free(left_data);
                        }
                    }
                    left_map_int.clear();

                    //free data in right map
                    for(auto const &right_pair: right_map_int){
                        for(void* right_data: right_pair.second){
                            free(right_data);
                        }
                    }
                    right_map_int.clear();

                    //free data in output
                    for(void* data: output){
                        free(data);
                    }
                    output.clear();

                    return -1;
                }
            }
            data = output.at(curr_output_index);
        }
        return 0;
    }


    RC BNLJoin::joinTwoTables_TypeInt(std::unordered_map<int, std::vector<void*>> &left_map, std::unordered_map<int, std::vector<void*>> &right_map, std::vector<void*> &output){
        for(auto const &left_pair: left_map){
            int left_key = left_pair.first;
            for(auto const &right_pair: right_map){
                int right_key = right_pair.first;
                if(left_key == right_key){
                    for(void* left_data : left_pair.second){
                        for(void* right_data: right_pair.second){
                            void* output_data = nullptr;
                            joinTwoData(left_data, right_data, output_data);
                            output.push_back(output_data);
                        }
                    }
                }
            }
        }
    }

    RC BNLJoin::joinTwoData(void* &left_data, void* &right_data, void* & output_data){
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        int left_data_size = getSizeOfData(left_data, this->leftIn_attrs);
        int right_data_size = getSizeOfData(right_data, this->rightIn_attrs);

        int left_numberOfCols = this->leftIn_attrs.size();
        int right_numberOfCols = this->rightIn_attrs.size();
        int output_numberOfCols = this->joined_attrs.size();

        int left_bitMapSize = ceil((float)left_numberOfCols/8);
        int right_bitMapSize = ceil((float)right_numberOfCols/8);
        int output_bitMapSize = ceil((float)output_numberOfCols/8);

        int output_data_size = left_data_size + right_data_size - left_bitMapSize - right_bitMapSize + output_bitMapSize;

        output_data = malloc(output_data_size);
        memset(output_data, 0, output_data_size);
        void* output_bitMap = malloc(output_bitMapSize*sizeof (char));

        memset(output_bitMap, 0, output_bitMapSize*sizeof (char));
        createOutPutBitMap(output_bitMap, left_data, right_data, output_bitMapSize);

        int offset = 0;
        memcpy((char*)output_data + offset, output_bitMap, output_bitMapSize);
        free(output_bitMap);
        offset += output_bitMapSize;

        memcpy((char*)output_data + offset, (char*)left_data + left_bitMapSize, left_data_size - left_bitMapSize);
        offset += (left_data_size - left_bitMapSize);

        memcpy((char*)output_data + offset, (char*)right_data + right_bitMapSize, right_data_size - right_bitMapSize);

        offset += (right_data_size - right_bitMapSize);
    }

    RC BNLJoin::createOutPutBitMap(void* &output_bitMap, void* &left_data, void* &right_data, int &output_bitMapSize){
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        int left_numberOfCols = this->leftIn_attrs.size();

        std::vector<bool> left_nullIndicator;
        for(int k = 0; k < left_numberOfCols; k++){
            bool isNull = rbfm.isColValueNull(left_data, k);
            left_nullIndicator.push_back(isNull);
        }

        int right_numberOfCols = this->rightIn_attrs.size();
        std::vector<bool> right_nullIndicator;

        for(int k = 0; k < right_numberOfCols; k++){
            bool isNull = rbfm.isColValueNull(right_data, k);
            right_nullIndicator.push_back(isNull);
        }

        std::vector<std::bitset<8>> temp_bitMap(output_bitMapSize);
        int j = 0;
        for(int i = 0; i < left_nullIndicator.size(); i++){
            if(left_nullIndicator.at(i)){
                temp_bitMap[j/8].set(8 - j%8 - 1);
            }
            j++;
        }
        for(int i = 0 ; i < right_nullIndicator.size(); i++){
            if(right_nullIndicator.at(i)){
                temp_bitMap[j/8].set(8 - j%8 - 1);
            }
            j++;
        }
        for(int i = 0; i < output_bitMapSize; i++){
            char* pointer = (char*)&temp_bitMap[i];
            memcpy((char*)output_bitMap + i, pointer, sizeof (char));
        }
    }

    RC BNLJoin::loadTuplesLeftTable_TypeInt(std::unordered_map<int, std::vector<void*>> &map){
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        int totalsize = 0;
        int count = 0;
        while(totalsize < this->bnl_numPages*PAGE_SIZE){
            void* data = malloc(PAGE_SIZE);
            count++;
            if(this->bnl_leftIn->getNextTuple(data)){
                free(data);
                this->finished_scan_left_table = true;
                break;
            }
            int sizeOfData = getSizeOfData(data, this->leftIn_attrs);
            totalsize += sizeOfData;
            void* copy_data = malloc(sizeOfData);
            memcpy(copy_data, data, sizeOfData);
            free(data);
            void* read_attr = nullptr;
            FileHandle dummy_fileHandle;
            RID dummy_rid;
            rbfm.readAttributeFromRecord(dummy_fileHandle, this->leftIn_attrs, dummy_rid, this->bnl_condition.lhsAttr, read_attr, copy_data);
            int key = *(int*)((char*)read_attr + sizeof (char ));
            if(map.find(key) != map.end()){
                map[key].push_back(copy_data);
            }else{
                std::vector<void*> v;
                v.push_back(copy_data);
                map[key] = v;
            }
        }
    }

    RC BNLJoin::loadTuplesRightTable_TypeInt(std::unordered_map<int, std::vector<void*>> &map){
        RecordBasedFileManager& rbfm = RecordBasedFileManager::instance();
        int totalsize = 0;
        int count = 0;
        while(totalsize < PAGE_SIZE){
            void* data = malloc(PAGE_SIZE);
            count++;
            if(this->bnl_rightIn->getNextTuple(data)){
                free(data);
                this->finished_scan_right_table = true;
                break;
            }
            int sizeOfData = getSizeOfData(data, this->rightIn_attrs);
            totalsize += sizeOfData;
            void* copy_data = malloc(sizeOfData);
            memcpy(copy_data, data, sizeOfData);
            free(data);
            void* read_attr = nullptr;
            FileHandle dummy_fileHandle;
            RID dummy_rid;
            rbfm.readAttributeFromRecord(dummy_fileHandle, this->rightIn_attrs, dummy_rid, this->bnl_condition.rhsAttr, read_attr, copy_data);
            int key = *(int*)((char*)read_attr + sizeof (char ));
            if(map.find(key) != map.end()){
                map[key].push_back(copy_data);
            }else{
                std::vector<void*> v;
                v.push_back(copy_data);
                map[key] = v;
            }
        }

    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        if(attrs.size() != 0) attrs.clear();
        std::vector<Attribute> leftIn_attr;
        std::vector<Attribute> rightIn_attr;
        this->bnl_leftIn->getAttributes(leftIn_attr);
        this->bnl_rightIn->getAttributes(rightIn_attr);

        for(Attribute attribute: leftIn_attr){
            attrs.push_back(attribute);
        }
        for(Attribute attribute: rightIn_attr){
            attrs.push_back(attribute);
        }
        return 0;
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
