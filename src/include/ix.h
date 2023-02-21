#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

# define IX_EOF (-1)  // end of the index scan

namespace PeterDB {

    struct IndexNodeMetadata{
        int freeSpace;
        int numOfKeys;
        int type;

        IndexNodeMetadata(){
            this->freeSpace = 0;
            this->numOfKeys = 0;
            this->type = 0;
        }
        IndexNodeMetadata(int freeSpace, int numOfKeys, int type){
            this->freeSpace = freeSpace;
            this->numOfKeys = numOfKeys;
            this->type = type;
        }
    };

    struct LeafNodeMetadata{
        int rightSibling;
        int freeSpace;
        int numOfKeys;
        int type;

        LeafNodeMetadata(){
            this->rightSibling = -1;
            this->freeSpace = 0;
            this->numOfKeys = 0;
            this->type = 1;
        }
        LeafNodeMetadata(int rightSibling, int freeSpace, int numOfKeys, int type){
            this->rightSibling = rightSibling;
            this->freeSpace = freeSpace;
            this->numOfKeys = numOfKeys;
            this->type = type;
        }
    };

    class IX_ScanIterator;

    class IXFileHandle;

    class IndexManager {

    public:
        static IndexManager &instance();

        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);

        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;

    protected:
        IndexManager() = default;                                                   // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

    private:
        int get_root_page_index(IXFileHandle ixFileHandle);
        int appendNewIndexPage(IXFileHandle &ixFileHandle, AttrType keyType, const void *key);
        //int appendNewLeafPageWithData(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid, int rightSibling);
        int appendNewLeafPageWithData(IXFileHandle &ixFileHandle, void* &data, int &data_len, int &rightSibling, int &freeSpace, int &num_keys, bool get_smallest_key, AttrType key_type, void* &smallest_key, int &len_smallest_key);
        int appendNewIndexPageWithData(IXFileHandle &ixFileHandle, void* &data, int &data_len, int &freeSpace, int &num_keys);
        int get_length_of_key(AttrType type, const void *key);
        int update_root_entry_dummy_page(IXFileHandle ixFileHandle, int rootIndex);
        RC updatePointerInParentNode(IXFileHandle &ixFileHandle, int parentIndex, bool isLeftPointer, int pointerVal, int index_of_key, AttrType keyType);
        RC insert_util(IXFileHandle &ixFileHandle, int node_page_index, AttrType keyType, const void *key, const RID &rid, void* &newChildEntry);
        bool isInternalNode(IXFileHandle &ixFileHandle, int node_page_index);
        int appendEmptyLeafPage(IXFileHandle &ixFileHandle, int rightSibling);
        int get_page_pointer_offset_for_insertion(void* &page, int node_page_index, AttrType keyType, const void *key);
        bool hasSpaceInLeafNode(void* &page, AttrType keyType, const void *key);
        RC put_entry_in_leaf_node(void* &page, AttrType keyType, const void *key, const RID &rid);
        RC put_entry_in_index_node(void* &page, AttrType keyType, void* &newChildEntry);
        RC splitLeafNode(void* &page, IXFileHandle &ixFileHandle, AttrType keyType, const void *key, const RID &rid, void* &newChildEntry);
        RC splitIndexNode(void* &page, IXFileHandle &ixFileHandle, AttrType keyType, void* &newChildEntry);
        RC find_offset_for_putting_key_in_leafNode(void* &page, AttrType keyType, const void *key, int& num_of_keys, int &required_space, int &keys_inspected, int &offset);
        RC find_offset_for_putting_key_in_indexNode(void* &page, AttrType keyType, void* &newChildEntry, int& num_of_keys, int &required_space, int &keys_inspected, int &offset);
        RC complete_data_update_already_existing_leaf(void* &page, void* &data, int &data_len, int &rightSibling, int &freeSpace, int &num_keys);
        RC complete_data_update_already_existing_index_node(void* &page, void* &data, int &data_len, int &freeSpace, int &num_keys);
        RC get_smallest_key_value_on_leaf_page(void* &page, AttrType key_type, void* &smallest_key, int &len_key);
        bool hasSpaceIndexNode(void* &page, AttrType keyType, void* &newChildEntry);
        RC extractKeyFromNewChildEntry(void* &newChildEntry, void* &extracted_key, int& rightPointer, AttrType type);
    };

    class IX_ScanIterator {
    public:

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
    };

    class IXFileHandle {
    public:

        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;

        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();

        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
        RC setHandle(FileHandle fileHandle);
        FileHandle getHandle();

        RC readPage(PageNum pageNum, void *data);
        RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
        RC appendPage(const void *data);
        unsigned getNumberOfPages();

    private:
        FileHandle fileHandle;

    };
}// namespace PeterDB
#endif // _ix_h_
