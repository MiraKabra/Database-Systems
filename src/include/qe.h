#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <string>
#include <limits>
#include <cstring>
#include <unordered_map>
#include "rm.h"
#include "ix.h"
#include "rbfm.h"

namespace PeterDB {

#define QE_EOF (-1)  // end of the index scan
    typedef enum AggregateOp {
        MIN = 0, MAX, COUNT, SUM, AVG
    } AggregateOp;

    // The following functions use the following
    // format for the passed data.
    //    For INT and REAL: use 4 bytes
    //    For VARCHAR: use 4 bytes for the length followed by the characters

    typedef struct Value {
        AttrType type;          // type of value
        void *data;             // value
    } Value;

    typedef struct Condition {
        std::string lhsAttr;        // left-hand side attribute
        CompOp op;                  // comparison operator
        bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
        std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
        Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE
    } Condition;

    class Iterator {
        // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;

        virtual RC getAttributes(std::vector<Attribute> &attrs) const = 0;

        virtual ~Iterator() = default;
    };

    class TableScan : public Iterator {
        // A wrapper inheriting Iterator over RM_ScanIterator
    private:
        RelationManager &rm;
        RM_ScanIterator iter;
        std::string tableName;
        std::vector<Attribute> attrs;
        std::vector<std::string> attrNames;
        RID rid;
    public:
        TableScan(RelationManager &rm, const std::string &tableName, const char *alias = NULL) : rm(rm) {
            //Set members
            this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            for (const Attribute &attr : attrs) {
                // convert to char *
                attrNames.push_back(attr.name);
            }

            // Call RM scan to get an iterator
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator() {
            iter.close();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);
        };

        RC getNextTuple(void *data) override {
            return iter.getNextTuple(rid, data);
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;

            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
            return 0;
        };

        ~TableScan() override {
            iter.close();
        };
    };

    class IndexScan : public Iterator {
        // A wrapper inheriting Iterator over IX_IndexScan
    private:
        RelationManager &rm;
        RM_IndexScanIterator iter;
        std::string tableName;
        std::string attrName;
        std::vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;
    public:
        IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName,
                  const char *alias = NULL) : rm(rm) {
            // Set members
            this->tableName = tableName;
            this->attrName = attrName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
            iter.close();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive, iter);
        };

        RC getNextTuple(void *data) override {
            RC rc = iter.getNextEntry(rid, key);
            if (rc == 0) {
                rc = rm.readTuple(tableName, rid, data);
            }
            return rc;
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;


            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
        };

        ~IndexScan() override {
            iter.close();
        };
    };

    class Filter : public Iterator {
        // Filter operator
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );

        ~Filter() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
        bool is_record_satisfiable(void* data, Condition& cond);
    private:
        Iterator* itr;
        Condition cond;
        std::vector<Attribute> attributes;
    };

    class Project : public Iterator {
        // Projection operator
    public:
        Project(Iterator *input,                                // Iterator of input R
                const std::vector<std::string> &attrNames);     // std::vector containing attribute names
        ~Project() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
        RC create_data_with_required_attributes(void* &total_data, void* &data);
        RC set_bitmap(void* &total_data, void* &bitmap);
        AttrType get_attribute_type(std::string attributeName);
    private:
        Iterator* itr;
        Condition cond;
        std::vector<Attribute> record_descriptor;
        std::vector<std::string> relative_attrNames;
    };

    class BNLJoin : public Iterator {
        // Block nested-loop join operator
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
                TableScan *rightIn,           // TableScan Iterator of input S
                const Condition &condition,   // Join condition
                const unsigned numPages       // # of pages that can be loaded into memory,
                //   i.e., memory block size (decided by the optimizer)
        );

        ~BNLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
        RC loadTuplesLeftTable_TypeInt(std::unordered_map<int, std::vector<void*>> &map);
        RC loadTuplesLeftTable_TypeReal(std::unordered_map<float, std::vector<void*>> &map);
        RC loadTuplesRightTable_TypeInt(std::unordered_map<int, std::vector<void*>> &map);
        RC loadTuplesRightTable_TypeReal(std::unordered_map<float, std::vector<void*>> &map);
        int getSizeOfData(void* &data, std::vector<Attribute> &recordDescriptor);
        RC joinTwoTables_TypeInt(std::unordered_map<int, std::vector<void*>> &left_map, std::unordered_map<int, std::vector<void*>> &right_map, std::vector<void*> &output);
        RC joinTwoTables_TypeReal(std::unordered_map<float, std::vector<void*>> &left_map, std::unordered_map<float, std::vector<void*>> &right_map, std::vector<void*> &output);
        RC joinTwoData(void* &leftData, void* &rightData, void* & outputData);
        RC createOutPutBitMap(void* &output_bitMap, void* &left_data, void* &right_data, int &output_bitMapSize);
    private:
        Iterator *bnl_leftIn = nullptr;
        TableScan *bnl_rightIn = nullptr;
        TableScan *bnl_righIn_initial = nullptr;
        Condition bnl_condition;
        unsigned bnl_numPages = 0;
        std::vector<Attribute> leftIn_attrs;
        std::vector<Attribute> rightIn_attrs;
        std::vector<Attribute> joined_attrs;
        AttrType join_keyType;
        bool start = true;

        std::unordered_map<int, std::vector<void*>> left_map_int;
        std::unordered_map<int, std::vector<void*>> right_map_int;
        std::unordered_map<float, std::vector<void*>> left_map_real;
        std::unordered_map<float, std::vector<void*>> right_map_real;

        std::vector<void*> output;
        int curr_output_index = -1;
        bool finished_scan_left_table = false;
        bool finished_scan_right_table = false;
    };

    class INLJoin : public Iterator {
        // Index nested-loop join operator
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
                IndexScan *rightIn,          // IndexScan Iterator of input S
                const Condition &condition   // Join condition
        );

        ~INLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
        RC joinTwoData(void* &leftData, void* &rightData, void* & outputData);
        RC createOutPutBitMap(void* &output_bitMap, void* &left_data, void* &right_data, int &output_bitMapSize);
        int getSizeOfData(void* &data, std::vector<Attribute> &recordDescriptor);
    private:
        Iterator *inl_leftIn = nullptr;
        IndexScan *inl_rightIn = nullptr;
        Condition inl_condition;
        std::vector<Attribute> leftIn_attrs;
        std::vector<Attribute> rightIn_attrs;
        std::vector<Attribute> joined_attrs;
        AttrType join_keyType;
        bool finished_index_scan_for_curr_tuple = false;
        bool start = true;
        void* leftData = nullptr;
    };

    // 10 extra-credit points
    class GHJoin : public Iterator {
        // Grace hash join operator
    public:
        GHJoin(Iterator *leftIn,               // Iterator of input R
               Iterator *rightIn,               // Iterator of input S
               const Condition &condition,      // Join condition (CompOp is always EQ)
               const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
        );

        ~GHJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class Aggregate : public Iterator {
        // Aggregation operator
    public:
        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
                  const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        );

        ~Aggregate() override;

        RC getNextTuple(void *data) override;

        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrName = "MAX(rel.attr)"
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    private:
        Iterator *aggr_input = nullptr;
        Attribute  aggr_attr;
        Attribute group_attr;
        AggregateOp aggr_op;
        std::vector<Attribute> input_attrs;
        bool end = false;
        bool group_by = false;
        int curr_index = -1;
        std::unordered_map<int, int> map;
        std::vector<int> vec;
    };
} // namespace PeterDB

#endif // _qe_h_
