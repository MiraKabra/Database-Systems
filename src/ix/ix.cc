#include "src/include/ix.h"
#include <float.h>
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
            /*
             * Insert leaf page with this key
             * */
            int key_len = get_length_of_key(attribute.type, key);
            //Deduct 4*sizeof (unsigned ) for metadata
            //Deduct length of key
            //Deduct 2*sizeof (unsigned ) for rid (pagenum, slotnum)
            int freeSpace = PAGE_SIZE - 4*sizeof (unsigned ) - key_len - 2*sizeof (unsigned );
            int num_keys = 1;
            int data_len = key_len + 2*sizeof (unsigned );
            void* data = malloc(data_len);
            int offset = 0;
            memcpy((char*)data + offset, key, key_len);
            offset += key_len;
            memcpy((char*)data + offset, &rid.pageNum, sizeof (unsigned ));
            offset += sizeof (unsigned );
            memcpy((char*)data + offset, &rid.slotNum, sizeof (unsigned ));
            int rightSibling = -1;
            void* smallest_key = nullptr;
            int len_smallest_key = 0;
            int rightPointer = appendNewLeafPageWithData(ixFileHandle, data, data_len, rightSibling, freeSpace, num_keys, false, attribute.type, smallest_key, len_smallest_key);

//            int rightPointer = appendNewLeafPageWithData(ixFileHandle, attribute, key, rid, -1);

            free(data);
            int leftPointer = appendEmptyLeafPage(ixFileHandle, rightPointer);
            //Add the pointer in the index node
            updatePointerInParentNode(ixFileHandle, root_page_index, true,
                                      leftPointer, 0, attribute.type);
            updatePointerInParentNode(ixFileHandle, root_page_index, false, rightPointer, 0, attribute.type);

            return 0;
        }
        void* newChildEntry = nullptr;
        insert_util(ixFileHandle, root_page_index, attribute.type, key, rid, newChildEntry);
        return 0;
    }

    int IndexManager::appendEmptyLeafPage(IXFileHandle &ixFileHandle, int rightSibling){
        void* page = malloc(PAGE_SIZE);
        memset(page, 0, PAGE_SIZE);
        int numPages = ixFileHandle.getNumberOfPages();
        LeafNodeMetadata leaf_metadata;
        //Deduct 4*sizeof (unsigned ) for metadata
        //Deduct length of key
        //Deduct 2*sizeof (unsigned ) for rid (pagenum, slotnum)
        int updated_freespace = PAGE_SIZE - 4*sizeof (unsigned );
        leaf_metadata = {rightSibling, updated_freespace, 0, 1};

        //put metadata in page
        int offset = PAGE_SIZE - 4*sizeof (unsigned );
        memcpy((char*)page + offset, &leaf_metadata.rightSibling, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy((char*)page + offset, &leaf_metadata.freeSpace, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy((char*)page + offset, &leaf_metadata.numOfKeys, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy((char*)page + offset, &leaf_metadata.type, sizeof (unsigned ));

        ixFileHandle.appendPage(page);
        free(page);
        return numPages;
    }

    RC IndexManager::insert_util(IXFileHandle &ixFileHandle, int node_page_index, AttrType keyType, const void *key, const RID &rid, void* &newChildEntry){
        void* page = malloc(PAGE_SIZE);
        ixFileHandle.readPage(node_page_index, page);
        if(isInternalNode(ixFileHandle, node_page_index)){
            int offset_for_pointer_page = get_page_pointer_offset_for_insertion(page, node_page_index, keyType, key);
            insert_util(ixFileHandle, *(int*)((char*)page + offset_for_pointer_page), keyType, key, rid, newChildEntry);
        //Implement rest of it

        }else{
            if(hasSpaceInLeafNode(page, keyType, key)){
                put_entry_in_leaf_node(page, keyType, key, rid);
                ixFileHandle.writePage(node_page_index, page);
                if(newChildEntry != nullptr){
                    free(newChildEntry);
                }
                newChildEntry = nullptr;
                return 0;
            }else{
                splitLeafNode(page, ixFileHandle, keyType, key, rid, newChildEntry);
                if(ixFileHandle.writePage(node_page_index, page)) return -1;
            }
        }
        return 0;
    }

    RC IndexManager::splitLeafNode(void* &page, IXFileHandle &ixFileHandle, AttrType keyType, const void *key, const RID &rid, void* &newChildEntry){
        void* temp_page = malloc(2*PAGE_SIZE);
        LeafNodeMetadata old_node_metadata;
        int offset = PAGE_SIZE - 4*sizeof (unsigned );
        memcpy(&old_node_metadata.rightSibling, (char*)page + offset, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy(&old_node_metadata.freeSpace, (char*)page + offset, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy(&old_node_metadata.numOfKeys, (char*)page + offset, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy(&old_node_metadata.type, (char*)page + offset, sizeof (unsigned ));

        int size_of_data_entry = PAGE_SIZE - old_node_metadata.freeSpace - 4*sizeof (unsigned );

        //copy data to temp_page
        memcpy(temp_page, page, size_of_data_entry);

        memset(page, 0, size_of_data_entry);

        //Put entry in temp_page
        offset = 0;
        int keys_inspected = 0;
        int required_space = 0;

        find_offset_for_putting_key_in_leafNode(temp_page, keyType, key, old_node_metadata.numOfKeys, required_space, keys_inspected, offset);

        /*
         * Found the offset where to insert,
         * check if it is getting inserted at the end of all data entries
         * because in that case we won't need to shift anything*/
        if(keys_inspected != old_node_metadata.numOfKeys){
            //shift
            memmove((char*)temp_page + offset + required_space, (char*)page + offset, size_of_data_entry - offset);
        }
        //put key
        if(keyType == TypeInt){
            memcpy((char*)temp_page + offset, key, sizeof (unsigned ));
            offset += sizeof (unsigned );
            memcpy((char*)temp_page + offset, &rid.pageNum, sizeof (unsigned ));
            offset += sizeof (unsigned );
            memcpy((char*)temp_page + offset, &rid.slotNum, sizeof (unsigned ));
            size_of_data_entry += 3*sizeof (unsigned );
        }else if(keyType == TypeReal){
            memcpy((char*)temp_page + offset, key, sizeof (float ));
            offset += sizeof (float );
            memcpy((char*)temp_page + offset, &rid.pageNum, sizeof (unsigned ));
            offset += sizeof (unsigned );
            memcpy((char*)temp_page + offset, &rid.slotNum, sizeof (unsigned ));
            size_of_data_entry = size_of_data_entry + sizeof (float ) + 2*sizeof (unsigned );
        }else{
            int key_len = *(int*)(key);
            memcpy((char*)temp_page + offset, key, sizeof (unsigned ) + key_len);
            offset += sizeof (unsigned ) + key_len;
            memcpy((char*)temp_page + offset, &rid.pageNum, sizeof (unsigned ));
            offset += sizeof (unsigned );
            memcpy((char*)temp_page + offset, &rid.slotNum, sizeof (unsigned ));
            size_of_data_entry = size_of_data_entry + 3*sizeof (unsigned ) + key_len;
        }
        /*
         * Put first half (lesser amount) in old page
         * Put the rest in a new leaf page*/

        int num_keys_old_page = 0;
        int num_keys_new_page = old_node_metadata.numOfKeys + 1;

        int free_space_old_page = PAGE_SIZE - 4*sizeof (unsigned );
        int free_space_new_page = PAGE_SIZE - 4*sizeof (unsigned );
        int half_data_offset = 0;
        if(keyType == TypeInt){
            while(true){
                int next_addition = 3*sizeof (unsigned );
                if(half_data_offset + next_addition > size_of_data_entry/2){
                    break;
                }
                half_data_offset += next_addition;
                num_keys_old_page++;
                num_keys_new_page--;
                free_space_old_page -= next_addition;
            }
        }else if(keyType == TypeReal){
            while(true){
                int next_addition = sizeof (float) + 2*sizeof (unsigned );
                if(half_data_offset + next_addition > size_of_data_entry/2){
                    break;
                }
                half_data_offset += next_addition;
                num_keys_old_page++;
                num_keys_new_page--;
                free_space_old_page -= next_addition;
            }
        }else{
            while(true){
                int next_varchar_len = *(int*)half_data_offset;
                int next_addition = sizeof (unsigned ) + next_varchar_len + 2*sizeof (unsigned );
                if(half_data_offset + next_addition > size_of_data_entry/2){
                    break;
                }
                half_data_offset += next_addition;
                num_keys_old_page++;
                num_keys_new_page--;
                free_space_old_page -= next_addition;
            }
        }
        free_space_new_page = free_space_new_page - (size_of_data_entry - half_data_offset);

        int new_leaf_data_len = (size_of_data_entry - half_data_offset);
        void* new_leaf_data = malloc(new_leaf_data_len);
        memcpy(new_leaf_data, (char*)temp_page + half_data_offset, new_leaf_data_len);

        void* smallest_key = nullptr;
        int len_smallest_key = 0;

        int new_leaf_index = appendNewLeafPageWithData(ixFileHandle, new_leaf_data, new_leaf_data_len, old_node_metadata.rightSibling, free_space_new_page, num_keys_new_page,
                                                       true, keyType, smallest_key, len_smallest_key);

        int old_leaf_data_len = half_data_offset;
        void* old_leaf_data = malloc(old_leaf_data_len);
        memcpy(old_leaf_data, temp_page, old_leaf_data_len);

        complete_data_update_already_existing_leaf(page, old_leaf_data, old_leaf_data_len, new_leaf_index, free_space_old_page, num_keys_old_page);

        newChildEntry = malloc(sizeof (unsigned ) + len_smallest_key);
        memcpy(newChildEntry, &new_leaf_index, sizeof (unsigned ));
        memcpy((char*)newChildEntry + sizeof (unsigned ), smallest_key, len_smallest_key);

        free(temp_page);
        return 0;
    }

    RC IndexManager::get_smallest_key_value_on_leaf_page(void* &page, AttrType key_type, void* &smallest_key, int &len_smallest_key){
        if(key_type == TypeInt ){
            len_smallest_key = sizeof (unsigned );
            smallest_key = malloc(len_smallest_key);
            memcpy(smallest_key, page, len_smallest_key);
        }else if(key_type == TypeReal){
            len_smallest_key = sizeof (float );
            smallest_key = malloc(len_smallest_key);
            memcpy(smallest_key, page, len_smallest_key);
        }
        else{
            int len = *(int*)page;
            len_smallest_key = sizeof (unsigned ) + len;
            smallest_key = malloc(len_smallest_key);
            memcpy(smallest_key, page,len_smallest_key);
        }
        return 0;
    }

    /*
     * This function does not memset the old data of page to zero, do that before calling this function
     * */
    RC IndexManager::complete_data_update_already_existing_leaf(void* &page, void* &data, int &data_len, int &rightSibling, int &freeSpace, int &num_keys){
        LeafNodeMetadata leaf_metadata;
        leaf_metadata = {rightSibling, freeSpace, num_keys, 1};

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
        memcpy(page, data, data_len);
        return 0;
    }

    int IndexManager::appendNewLeafPageWithData(IXFileHandle &ixFileHandle, void* &data, int &data_len, int &rightSibling, int &freeSpace, int &num_keys, bool get_smallest_key, AttrType key_type, void* &smallest_key, int &len_smallest_key){

        void* page = malloc(PAGE_SIZE);
        memset(page, 0, PAGE_SIZE);
        int numPages = ixFileHandle.getNumberOfPages();

        LeafNodeMetadata leaf_metadata;
        leaf_metadata = {rightSibling, freeSpace, num_keys, 1};

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
        memcpy(page, data, data_len);
        if(get_smallest_key){
            get_smallest_key_value_on_leaf_page(page, key_type, smallest_key, len_smallest_key);
        }
        ixFileHandle.appendPage(page);
        free(page);
        return numPages;
    }

    RC IndexManager::find_offset_for_putting_key_in_leafNode(void* &page, AttrType keyType, const void *key, int& num_of_keys, int &required_space, int &keys_inspected, int &offset){
        if(keyType == TypeInt){
            required_space = 3*sizeof (unsigned );
            int key_to_put = *(int*)key;
            while(keys_inspected < num_of_keys){
                int curr_key_under_inspection = *(int*)((char*)page + offset);
                if(key_to_put < curr_key_under_inspection){
                    break;
                }
                keys_inspected++;
                offset += 3*sizeof (unsigned );
            }
        }else if(keyType == TypeReal){
            required_space = sizeof (float) + 2*sizeof (unsigned );
            float key_to_put = *(float*)key;
            while(keys_inspected < num_of_keys){
                float curr_key_under_inspection = *(float*)((char*)page + offset);
                if(key_to_put < curr_key_under_inspection){
                    break;
                }
                keys_inspected++;
                offset += 2*sizeof (unsigned ) + sizeof (float);
            }
        }else{
            int key_to_put_len = *(int*)key;
            char key_to_put[key_to_put_len + 1];
            memcpy(&key_to_put, (char*)key + sizeof (unsigned ), key_to_put_len);
            key_to_put[key_to_put_len] = '\0';

            required_space = sizeof (unsigned ) + key_to_put_len + 2*sizeof (unsigned );
            while(keys_inspected < num_of_keys){
                int len_curr_key_under_inspection = *(int*)((char*)page + offset);
                char curr_key_under_inspection[len_curr_key_under_inspection + 1];

                memcpy(&curr_key_under_inspection, (char*)page + offset + sizeof (unsigned ), len_curr_key_under_inspection);
                curr_key_under_inspection[len_curr_key_under_inspection] = '\0';
                if(strcmp(key_to_put , curr_key_under_inspection) < 0){
                    break;
                }
                keys_inspected++;
                offset += sizeof (unsigned ) + len_curr_key_under_inspection + 2*sizeof (unsigned );
            }
        }
        return 0;
    }

    RC IndexManager::put_entry_in_leaf_node(void* &page, AttrType keyType, const void *key, const RID &rid){
        int offset = PAGE_SIZE - 2*sizeof (unsigned );
        int num_of_keys = *(int*)((char*)page + offset);
        offset = offset - sizeof (unsigned );
        int free_space = *(int*)((char*)page + offset);
        //deduct metadata size
        int size_of_data_entry = PAGE_SIZE - free_space - 4*sizeof (unsigned );
        offset = 0;
        int keys_inspected = 0;
        int required_space = 0;

        //Calculate offset for putting key
        find_offset_for_putting_key_in_leafNode(page, keyType, key, num_of_keys, required_space, keys_inspected, offset);
        /*
         * Found the offset where to insert,
         * check if it is getting inserted at the end of all data entries
         * because in that case we won't need to shift anything*/
        if(keys_inspected != num_of_keys){
            //shift
            memmove((char*)page + offset + required_space, (char*)page + offset, size_of_data_entry - offset);
        }
        //put key
        if(keyType == TypeInt){
            memcpy((char*)page + offset, key, sizeof (unsigned ));
            offset += sizeof (unsigned );
            memcpy((char*)page + offset, &rid.pageNum, sizeof (unsigned ));
            offset += sizeof (unsigned );
            memcpy((char*)page + offset, &rid.slotNum, sizeof (unsigned ));
            free_space = free_space - 3*sizeof (unsigned );
        }else if(keyType == TypeReal){
            memcpy((char*)page + offset, key, sizeof (float ));
            offset += sizeof (float );
            memcpy((char*)page + offset, &rid.pageNum, sizeof (unsigned ));
            offset += sizeof (unsigned );
            memcpy((char*)page + offset, &rid.slotNum, sizeof (unsigned ));
            free_space = free_space - sizeof (float ) - 2*sizeof (unsigned );
        }else{
            int key_len = *(int*)(key);
            memcpy((char*)page + offset, key, sizeof (unsigned ) + key_len);
            offset += sizeof (unsigned ) + key_len;
            memcpy((char*)page + offset, &rid.pageNum, sizeof (unsigned ));
            offset += sizeof (unsigned );
            memcpy((char*)page + offset, &rid.slotNum, sizeof (unsigned ));
            free_space = free_space - 3*sizeof (unsigned ) - key_len;
        }
        //update freespace
        memcpy((char*)page + PAGE_SIZE - 3*sizeof (unsigned ), &free_space, sizeof (unsigned ));
        //update numkeys
        num_of_keys++;
        memcpy((char*)page + PAGE_SIZE - 2*sizeof (unsigned ), &num_of_keys, sizeof (unsigned ));


        return 0;
    }
    bool IndexManager::hasSpaceInLeafNode(void* &page, AttrType keyType, const void *key){
        int free_space = *(int*)((char*)page + PAGE_SIZE - 3*sizeof (unsigned ));
        int len_key = get_length_of_key(keyType, key);
        //Add values for rid
        int total_len_required = len_key + 2*sizeof (unsigned );
        if(total_len_required <= free_space) return true;
        return false;
    }

    int IndexManager::get_page_pointer_offset_for_insertion(void* &page, int node_page_index, AttrType keyType, const void *key){
        int i = 0;
        int offset = 0;
        int num_of_keys =  *(int*)((char*)page + PAGE_SIZE - 2*sizeof (unsigned ));

        //will this ever happen ?
        if(num_of_keys == 0){

        }

        if(keyType == TypeInt){
            int key_val = *(int*)key;
            int leftVal = INT_MIN;
            int rightVal = *(int*)((char*)page + sizeof (unsigned ));
            while(!(key_val >= leftVal && key_val < rightVal)){
                i++;
                //No key found
                if(i+1 > num_of_keys){
                    break;
                }
                offset += 2*sizeof (unsigned );
                leftVal = *(int*)((char*)page + offset - sizeof (unsigned ));
                rightVal = *(int*)((char*)page + offset + sizeof (unsigned ));
            }
        }else if(keyType == TypeReal){
            float key_val = *(float*)key;
            float leftVal = -1*FLT_MAX; //min value of float
            float rightVal = *(float*)((char*)page + sizeof (unsigned ));
            while(!(key_val >= leftVal && key_val < rightVal)){
                i++;
                //No key found
                if(i+1 > num_of_keys){
                    break;
                }
                offset += sizeof (unsigned ) + sizeof (float);
                leftVal = *(float*)((char*)page + offset - sizeof (float ));
                rightVal = *(float*)((char*)page + offset + sizeof (unsigned ));
            }

        }else{
            int given_val_len = *(int*)key;
            char given_val[given_val_len + 1];
            memcpy(&given_val, (char*)key + sizeof (unsigned ), given_val_len);
            given_val[given_val_len] = '\0';
            char leftKey[PAGE_SIZE] = "";
            int right_key_len = *(int*)((char*)page + sizeof (unsigned ));
            char rightKey[PAGE_SIZE] = "";
            memcpy(&rightKey, (char*)page + 2*sizeof (unsigned ), right_key_len);
            while(!(strcmp(given_val, leftKey) >= 0 && strcmp(given_val, rightKey) < 0)){
                i++;
                if(i+1 > num_of_keys){
                    break;
                }
                offset += 2*sizeof(unsigned ) + right_key_len;
                memcpy(&leftKey, (char*)page + offset - right_key_len, right_key_len);
                leftKey[right_key_len] = '\0';
                right_key_len = *(int*)((char*)page + offset + sizeof (unsigned ));
                memcpy(&rightKey, (char*)page + offset + sizeof (unsigned ), right_key_len);
                rightKey[right_key_len] = '\0';
            }
        }
        return offset;
    }

    bool IndexManager::isInternalNode(IXFileHandle &ixFileHandle, int node_page_index){
        void* page = malloc(PAGE_SIZE);
        ixFileHandle.readPage(node_page_index, page);
        int type = *(int*)((char*)page + PAGE_SIZE - sizeof (unsigned ));
        if(type == 1) return false;
        return true;
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
        int key_len = get_length_of_key(attribute.type, key);
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


//    int IndexManager::appendNewLeafPageWithData(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid, int rightSibling){
//        void* page = malloc(PAGE_SIZE);
//        memset(page, 0, PAGE_SIZE);
//        int numPages = ixFileHandle.getNumberOfPages();
//        int key_len = get_length_of_key(attribute.type, key);
//        LeafNodeMetadata leaf_metadata;
//        //Deduct 4*sizeof (unsigned ) for metadata
//        //Deduct length of key
//        //Deduct 2*sizeof (unsigned ) for rid (pagenum, slotnum)
//        int freeSpace = PAGE_SIZE - 4*sizeof (unsigned ) - key_len - 2*sizeof (unsigned );
//        leaf_metadata = {rightSibling, freeSpace, 1, 1};
//
//        //put metadata in page
//        int offset = PAGE_SIZE - 4*sizeof (unsigned );
//        memcpy((char*)page + offset, &leaf_metadata.rightSibling, sizeof (unsigned ));
//        offset += sizeof (unsigned );
//        memcpy((char*)page + offset, &leaf_metadata.freeSpace, sizeof (unsigned ));
//        offset += sizeof (unsigned );
//        memcpy((char*)page + offset, &leaf_metadata.numOfKeys, sizeof (unsigned ));
//        offset += sizeof (unsigned );
//        memcpy((char*)page + offset, &leaf_metadata.type, sizeof (unsigned ));
//
//        //put data in page
//        offset = 0;
//        memcpy((char*)page + offset, key, key_len);
//        offset += key_len;
//        memcpy((char*)page + offset, &rid.pageNum, sizeof (unsigned ));
//        offset += sizeof (unsigned );
//        memcpy((char*)page + offset, &rid.slotNum, sizeof (unsigned ));
//        ixFileHandle.appendPage(page);
//        free(page);
//        return numPages;
//    }

    int IndexManager::get_length_of_key(AttrType type, const void *key){
        if(type == TypeInt){
            return sizeof (unsigned );
        }else if(type == TypeReal){
            return sizeof (float );
        }else if(type == TypeVarChar){
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