#include "src/include/ix.h"

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    /* Create an index file
     * The first page (after the hidden page) is reserved for dummy root
     * First page's last 4 byte contains pointer to root page (root page num)
     * This will be initially set to -1 implying tree is empty
     * */
    RC IndexManager::createFile(const std::string &fileName) {

        PagedFileManager& pagedFileManager = PagedFileManager::instance();
        if(pagedFileManager.createFile(fileName)) return -1;
        FileHandle indexFileHandle;
        if(pagedFileManager.openFile(fileName, indexFileHandle)) return -1;


        char* page = (char*)malloc(PAGE_SIZE);
        memset(page, 0, PAGE_SIZE);
        //indexFileHandle.readPage(0, page);
        int root = -1;
        memcpy(page + PAGE_SIZE - sizeof (unsigned ), &root,  sizeof (unsigned ));
        indexFileHandle.appendPage(page);

        if(pagedFileManager.closeFile(indexFileHandle)) return -1;
        return 0;
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        PagedFileManager& pagedFileManager = PagedFileManager::instance();
        if(pagedFileManager.destroyFile(fileName)) return -1;
        return 0;
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        PagedFileManager& pagedFileManager = PagedFileManager::instance();
        FileHandle indexFileHandle;
        if(pagedFileManager.openFile(fileName, indexFileHandle)) return -1;
        ixFileHandle.setHandle(indexFileHandle);
        return 0;
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        PagedFileManager& pagedFileManager = PagedFileManager::instance();
        FileHandle fl = ixFileHandle.getHandle();
        if(pagedFileManager.closeFile(fl)) return -1;
        return 0;
    }


    RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        int root_page_index = get_root_page_index(ixFileHandle);
        //If the tree is empty
        if(root_page_index == -1){
            root_page_index = appendNewIndexPage(ixFileHandle, attribute, key);
            //Update root page index in the dummy page;
            update_root_entry_dummy_page(ixFileHandle, root_page_index);
            //Insert leaf page
            int rightPointer = appendNewLeafPage(ixFileHandle, attribute, key, rid);
            //Add the pointer in the index node
            updatePointerInParentNode(ixFileHandle, root_page_index, false, rightPointer, 0, attribute.type);
            return 0;
        }

        return 0;
    }

    RC IndexManager::updatePointerInParentNode(IXFileHandle &ixFileHandle, int parentIndex, bool isLeftPointer, int pointerVal, int index_of_key, AttrType keyType){
        void* page = malloc(PAGE_SIZE);
        ixFileHandle.readPage(parentIndex, page);
        int offset = 0;
        if(keyType == TypeInt || keyType == TypeReal){
            int count = 0;
            while(count < index_of_key){
                count++;
                offset += 3*sizeof (unsigned );
            }
            if(isLeftPointer){
                memcpy((char*)page + offset, &pointerVal, sizeof (unsigned ));
            }else{
                //it's the right side pointer
                offset += 2*sizeof (unsigned );
                memcpy((char*)page + offset, &pointerVal, sizeof (unsigned ));
            }
        }else{
            int count = 0;
            while(count < index_of_key){
                count++;
                //Add left pointer length
                offset += sizeof (unsigned);

                int len = *(int*)((char*)page + offset);

                offset += sizeof (unsigned) + len;
                //Add right pointer length
                offset += sizeof (unsigned);
            }
            if(isLeftPointer){
                memcpy((char*)page + offset, &pointerVal, sizeof (unsigned ));
            }else{
                offset += sizeof (unsigned );
                int len = *(int*)((char*)page + offset);
                offset += sizeof (unsigned ) + len;
                memcpy((char*)page + offset, &pointerVal, sizeof (unsigned ));
            }
        }
        ixFileHandle.writePage(parentIndex, page);
        free(page);
        return 0;
    }

    int IndexManager::appendNewIndexPage(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key){
        void* page = malloc(PAGE_SIZE);
        memset(page, 0, PAGE_SIZE);
        int numPages = ixFileHandle.getNumberOfPages();
        int key_len = get_length_of_key(attribute, key);
        IndexNodeMetadata index_metadata;
        //Deduct 3*sizeof (unsigned ) for metadata
        //Deduct 2*sizeof (unsigned ) for two pointers
        //Deduct length of key
        int updated_freespace =  PAGE_SIZE - 3*sizeof (unsigned ) - 2*sizeof (unsigned ) - key_len;
        index_metadata = {updated_freespace, 1, 0};

        //put metadata in page
        int offset = PAGE_SIZE - 3*sizeof (unsigned );
        memcpy((char*)page + offset, &index_metadata.freeSpace, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy((char*)page + offset, &index_metadata.numOfKeys, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy((char*)page + offset, &index_metadata.type, sizeof (unsigned ));

        //put data in page
        int pointer_initial_val = -1;
        offset = 0;
        //copy left pointer
        memcpy((char*)page + offset, &pointer_initial_val, sizeof (unsigned ));
        offset += sizeof (unsigned );
        //copy key
        memcpy((char*)page + offset, key, key_len);
        offset += key_len;
        //copy right pointer
        memcpy((char*)page + offset, &pointer_initial_val, sizeof (unsigned ));
        ixFileHandle.appendPage(page);
        free(page);
        return numPages;
    }


    int IndexManager::appendNewLeafPage(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid){
        void* page = malloc(PAGE_SIZE);
        memset(page, 0, PAGE_SIZE);
        int numPages = ixFileHandle.getNumberOfPages();
        int key_len = get_length_of_key(attribute, key);
        LeafNodeMetadata leaf_metadata;
        //Deduct 4*sizeof (unsigned ) for metadata
        //Deduct length of key
        //Deduct 2*sizeof (unsigned ) for rid (pagenum, slotnum)
        int updated_freespace = PAGE_SIZE - 4*sizeof (unsigned ) - key_len - 2*sizeof (unsigned );
        leaf_metadata = {-1, updated_freespace, 1, 1};

        //put metadata in page
        int offset = PAGE_SIZE - 4*sizeof (unsigned );
        memcpy((char*)page + offset, &leaf_metadata.rightSibling, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy((char*)page + offset, &leaf_metadata.freeSpace, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy((char*)page + offset, &leaf_metadata.numOfKeys, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy((char*)page + offset, &leaf_metadata.type, sizeof (unsigned ));

        //put data in page
        offset = 0;
        memcpy((char*)page + offset, key, key_len);
        offset += key_len;
        memcpy((char*)page + offset, &rid.pageNum, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy((char*)page + offset, &rid.slotNum, sizeof (unsigned ));
        ixFileHandle.appendPage(page);
        free(page);
        return numPages;
    }

    int IndexManager::get_length_of_key(const Attribute &attribute, const void *key){
        if(attribute.type == TypeInt){
            return sizeof (unsigned );
        }else if(attribute.type == TypeReal){
            return sizeof (float );
        }else if(attribute.type == TypeVarChar){
            int len = *(int*)(key);
            return (sizeof (unsigned ) + len);
        }
        //Other types are not supported
        return -1;
    }

    int IndexManager::get_root_page_index(IXFileHandle ixFileHandle){
        void* page = malloc(PAGE_SIZE);
        if(ixFileHandle.readPage(0, page)){
            free(page);
            return -1;
        }
        int value = *(int*)((char*)page + PAGE_SIZE - sizeof (unsigned ));
        free(page);
        return value;
    }

    int IndexManager::update_root_entry_dummy_page(IXFileHandle ixFileHandle, int rootIndex){
        void* page = malloc(PAGE_SIZE);
        if(ixFileHandle.readPage(0, page)){
            free(page);
            return -1;
        }
        memcpy((char*)page + PAGE_SIZE - sizeof (unsigned ) , &rootIndex, sizeof (unsigned ));
        if(ixFileHandle.writePage(0, page)){
            free(page);
            return -1;
        }
        free(page);
        return 0;
    }
    RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        return -1;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        return -1;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
    }

    IX_ScanIterator::IX_ScanIterator() {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        return -1;
    }

    RC IX_ScanIterator::close() {
        return -1;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
    }

    IXFileHandle::~IXFileHandle() {
    }

    RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        this->getHandle().collectCounterValues(readPageCount, writePageCount, appendPageCount);
        return 0;
    }

    RC IXFileHandle::setHandle(FileHandle fileHandle){
        this->fileHandle = fileHandle;
    }
    FileHandle IXFileHandle::getHandle(){
        return this->fileHandle;
    }

    RC IXFileHandle::readPage(PageNum pageNum, void *data){
        if(this->getHandle().readPage(pageNum, data)) return -1;
        return 0;
    }

    RC IXFileHandle::writePage(PageNum pageNum, const void *data){
        if(this->getHandle().writePage(pageNum, data)) return -1;
        return 0;
    }

    RC IXFileHandle::appendPage(const void *data){
        if(this->getHandle().appendPage(data)) return -1;
        return 0;
    }

    unsigned IXFileHandle::getNumberOfPages(){
        return this->getHandle().getNumberOfPages();
    }
} // namespace PeterDB