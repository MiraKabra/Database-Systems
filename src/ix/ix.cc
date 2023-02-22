#include "src/include/ix.h"
#include <float.h>
#include <iostream>
#include <map>
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
        if(ixFileHandle.getHandle().getFile() != nullptr) return -1;
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
            root_page_index = appendNewIndexPage(ixFileHandle, attribute.type, key);
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

    RC IndexManager::extractKeyFromNewChildEntry(void* &newChildEntry, void* &extracted_key, int& rightPointer, AttrType type){
        if(type == TypeInt){
            extracted_key = malloc(sizeof (unsigned ));
            memcpy(extracted_key, newChildEntry, sizeof (unsigned ));
            rightPointer = *(int*)((char*)newChildEntry + sizeof (unsigned ));
        }else if(type == TypeReal){
            extracted_key = malloc(sizeof (float ));
            memcpy(extracted_key, newChildEntry, sizeof (float ));
            rightPointer = *(int*)((char*)newChildEntry + sizeof (float ));
        }else{
            int len = *(int*)newChildEntry;
            extracted_key = malloc(sizeof (unsigned ) + len);
            memcpy(extracted_key, newChildEntry, sizeof (unsigned ) + len);
            rightPointer = *(int*)((char*)newChildEntry + sizeof (unsigned ) + len);
        }
        return 0;
    }

    RC IndexManager::insert_util(IXFileHandle &ixFileHandle, int node_page_index, AttrType keyType, const void *key, const RID &rid, void* &newChildEntry){
        void* page = malloc(PAGE_SIZE);
        ixFileHandle.readPage(node_page_index, page);
        if(isInternalNode(page)){
            int offset_for_pointer_page = get_page_pointer_offset_for_insertion(page, node_page_index, keyType, key);
            insert_util(ixFileHandle, *(int*)((char*)page + offset_for_pointer_page), keyType, key, rid, newChildEntry);
        //Implement rest of it
            if(newChildEntry == nullptr)return 0;
            else{
                 //check if the index node has space for this new key
                 if(hasSpaceIndexNode(page, keyType, newChildEntry)){
                     put_entry_in_index_node(page, keyType, newChildEntry);
                     ixFileHandle.writePage(node_page_index, page);
                     if(newChildEntry != nullptr){
                         free(newChildEntry);
                     }
                     newChildEntry = nullptr;
                     return 0;
                 }else{
                     splitIndexNode(page, ixFileHandle, keyType, newChildEntry);
                     ixFileHandle.writePage(node_page_index, page);
                     //If a root page got split, create new node
                     if(get_root_page_index(ixFileHandle) == node_page_index){
                        void* extracted_key = nullptr;
                        int rightPointer = 0;
                        extractKeyFromNewChildEntry(newChildEntry, extracted_key, rightPointer, keyType);
                        int new_root_page_index = appendNewIndexPage(ixFileHandle, keyType, extracted_key);
                        update_root_entry_dummy_page(ixFileHandle, new_root_page_index);
                        updatePointerInParentNode(ixFileHandle, new_root_page_index, true, node_page_index, 0, keyType);
                         updatePointerInParentNode(ixFileHandle, new_root_page_index, false, rightPointer, 0, keyType);
                         free(newChildEntry);
                         return 0;
                     }
                 }
            }
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
    /*
     * Create a temp page of 2*PAGE_SIZE and insert the newChildEntry
     * Here interesting thing is that the push-up value will be inserted into newChildEntry
     * at the end*/
    RC IndexManager::splitIndexNode(void* &page, IXFileHandle &ixFileHandle, AttrType keyType, void* &newChildEntry){
        void* temp_page = malloc(2*PAGE_SIZE);
        IndexNodeMetadata old_node_metadata;
        int offset = PAGE_SIZE - 3*sizeof (unsigned );
        memcpy(&old_node_metadata.freeSpace, (char*)page + offset, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy(&old_node_metadata.numOfKeys, (char*)page + offset, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy(&old_node_metadata.type, (char*)page + offset, sizeof (unsigned ));

        int size_of_data_entry = PAGE_SIZE - old_node_metadata.freeSpace - 3*sizeof (unsigned );

        //copy data to temp_page
        memcpy(temp_page, page, size_of_data_entry);

        memset(page, 0, size_of_data_entry);

        //Put entry in temp_page
        offset = 0;
        int keys_inspected = 0;
        int required_space = 0;

        find_offset_for_putting_key_in_indexNode(temp_page, keyType, newChildEntry, old_node_metadata.numOfKeys, required_space, keys_inspected, offset);

        //Step c
        offset += sizeof (unsigned );

        /*
         * Found the offset where to insert,
         * check if it is getting inserted at the end of all data entries
         * because in that case we won't need to shift anything*/

        if(keys_inspected != old_node_metadata.numOfKeys){
            //shift right
            memmove((char*)temp_page + offset + required_space, (char*)temp_page + offset, size_of_data_entry - offset);
        }

        //put key
        memcpy((char*)temp_page + offset, newChildEntry, required_space);

        size_of_data_entry += required_space;

        /*
         * Put first half  in old page
         * copy the next one in newChildEntry
         * Put the rest in a new index node page (N2)
         * Store pointer to N2 also in newChildEntry*/
        int num_keys_old_page = 0;
        int num_keys_new_page = old_node_metadata.numOfKeys;

        int free_space_old_page = PAGE_SIZE - 3*sizeof (unsigned );
        int free_space_new_page = PAGE_SIZE - 3*sizeof (unsigned );
        int half_data_offset = 0;

        if(keyType == TypeInt){
            while(true){
                int next_addition = 2*sizeof (unsigned );
                if(half_data_offset + next_addition > size_of_data_entry/2){
                    break;
                }
                half_data_offset += next_addition;
                num_keys_old_page++;
                num_keys_new_page--;
                free_space_old_page -= next_addition;
            }
        }else if(keyType == TypeReal){
            while (true){
                int next_addition = sizeof (unsigned ) + sizeof (float);
                if(half_data_offset + next_addition > size_of_data_entry/2){
                    break;
                }
                half_data_offset += next_addition;
                num_keys_old_page++;
                num_keys_new_page--;
                free_space_old_page -= next_addition;
            }
        }else{
            while (true){
                int next_varchar_len = *(int*)((char*)half_data_offset + sizeof (unsigned ));
                int next_addition = 2*sizeof (unsigned ) + next_varchar_len;
                if(half_data_offset + next_addition > size_of_data_entry/2){
                    break;
                }
                half_data_offset += next_addition;
                num_keys_old_page++;
                num_keys_new_page--;
                free_space_old_page -= next_addition;
            }
        }
        //similar to step c
        half_data_offset += sizeof (unsigned );
        free_space_old_page -= sizeof (unsigned );

        int copy_key_len = 0;
        if(keyType == TypeInt){
            copy_key_len = sizeof (unsigned );
        }else if(keyType == TypeReal){
            copy_key_len = sizeof (float );
        }else{
            copy_key_len = *(int*)((char*)temp_page + half_data_offset);
            copy_key_len += sizeof (unsigned );
        }

        free(newChildEntry);
        newChildEntry = malloc(copy_key_len + sizeof (unsigned ));
        memcpy(newChildEntry, (char*)temp_page + half_data_offset, copy_key_len);

        int old_index_data_len = half_data_offset;
        void* old_index_data = malloc(old_index_data_len);
        memcpy(old_index_data, temp_page, old_index_data_len);

        complete_data_update_already_existing_index_node(page, old_index_data, old_index_data_len, free_space_old_page, num_keys_old_page);

        half_data_offset += copy_key_len;
        free_space_new_page = free_space_new_page - (size_of_data_entry - half_data_offset);
        int new_index_node_data_len = size_of_data_entry - half_data_offset;
        void* new_index_node_data = malloc(new_index_node_data_len);

        memcpy(new_index_node_data, (char*)temp_page + half_data_offset, new_index_node_data_len);

        int new_index_page = appendNewIndexPageWithData(ixFileHandle, new_index_node_data, new_index_node_data_len, free_space_new_page, num_keys_new_page);

        //copy this pagenum in newChildEntry
        memcpy((char*)newChildEntry + copy_key_len, &new_index_page, sizeof (unsigned ));

        free(temp_page);
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

        }else if(keyType == TypeReal){
            memcpy((char*)temp_page + offset, key, sizeof (float ));
            offset += sizeof (float );
        }else{
            int key_len = *(int*)(key);
            memcpy((char*)temp_page + offset, key, sizeof (unsigned ) + key_len);
            offset += sizeof (unsigned ) + key_len;
        }

        memcpy((char*)temp_page + offset, &rid.pageNum, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy((char*)temp_page + offset, &rid.slotNum, sizeof (unsigned ));

        size_of_data_entry += required_space;

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

        int new_leaf_index = appendNewLeafPageWithData(ixFileHandle, new_leaf_data, new_leaf_data_len, old_node_metadata.rightSibling, free_space_new_page, num_keys_new_page,true, keyType, smallest_key, len_smallest_key);

        int old_leaf_data_len = half_data_offset;
        void* old_leaf_data = malloc(old_leaf_data_len);
        memcpy(old_leaf_data, temp_page, old_leaf_data_len);

        complete_data_update_already_existing_leaf(page, old_leaf_data, old_leaf_data_len, new_leaf_index, free_space_old_page, num_keys_old_page);

        newChildEntry = malloc(len_smallest_key + sizeof (unsigned ));
        /*
         * newChildEntry format : key + 4 byte page index*/
        memcpy(newChildEntry, smallest_key, len_smallest_key);
        memcpy((char*)newChildEntry + len_smallest_key, &new_leaf_index, sizeof (unsigned ));

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
    RC IndexManager::complete_data_update_already_existing_index_node(void* &page, void* &data, int &data_len, int &freeSpace, int &num_keys){
        IndexNodeMetadata index_metadata;
        index_metadata = {freeSpace, num_keys, 0};

        //put metadata in page
        int offset = PAGE_SIZE - 3*sizeof (unsigned );

        memcpy((char*)page + offset, &index_metadata.freeSpace, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy((char*)page + offset, &index_metadata.numOfKeys, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy((char*)page + offset, &index_metadata.type, sizeof (unsigned ));

        //put data in page
        memcpy(page, data, data_len);
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

    int IndexManager::appendNewIndexPageWithData(IXFileHandle &ixFileHandle, void* &data, int &data_len, int &freeSpace, int &num_keys){

        void* page = malloc(PAGE_SIZE);
        memset(page, 0, PAGE_SIZE);
        int numPages = ixFileHandle.getNumberOfPages();

        IndexNodeMetadata index_metadata;

        index_metadata = {freeSpace, num_keys, 0};

        //put metadata in page
        int offset = PAGE_SIZE - 3*sizeof (unsigned );
        memcpy((char*)page + offset, &index_metadata.freeSpace, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy((char*)page + offset, &index_metadata.numOfKeys, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy((char*)page + offset, &index_metadata.type, sizeof (unsigned ));

        //put data in page
        memcpy(page, data, data_len);

        ixFileHandle.appendPage(page);
        free(page);
        return numPages;
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

    /*
     * newChildEntry format : key + 4 byte child page index
     * If this function is called, the index node has space for this entry
     *
     * We need to search the place for this insertion. The offset for insertion can be at the
     * beginning, in the middle or at the end
     *
     * In the beginning or middle :
     * a. shift is required
     * b. find offset: points to 0 in case of beginning
     * c. increase offset by 4 byte: this 4 byte pointer becomes the left side pointer of the new entry
     * d. shift the rest of the entry by length of newChildEntry
     * e. paste newChildEntry and update freespace, numkeys
     *
     * At the end:
     * a. no shift is needed
     * b. offset is at the end of k th key (start of k+1 pointer)
     * c. increase offset by 4 byte: this 4 byte pointer becomes the left side pointer of the new entry
     * c. Just paste the newchildentry, here the k+1 th pointer becomes the left side pointer of new key
     * d. update freespace, numkeys
     * */
    RC IndexManager::put_entry_in_index_node(void* &page, AttrType keyType, void* &newChildEntry){
        int offset = PAGE_SIZE - 2*sizeof (unsigned );
        int num_of_keys = *(int*)((char*)page + offset);
        offset = offset - sizeof (unsigned );
        int free_space = *(int*)((char*)page + offset);

        //deduct metadata size
        int size_of_data_entry = PAGE_SIZE - free_space - 3*sizeof (unsigned );
        offset = 0;
        int keys_inspected = 0;
        int required_space = 0;

        //calculate offset for putting key
        find_offset_for_putting_key_in_indexNode(page, keyType, newChildEntry, num_of_keys, required_space, keys_inspected, offset);
        //Step c
        offset += sizeof (unsigned );

        /*
         * Found the offset where to insert,
         * check if it is getting inserted at the end of all data entries
         * because in that case we won't need to shift anything*/

        if(keys_inspected != num_of_keys){
            //shift right
            memmove((char*)page + offset + required_space, (char*)page + offset, size_of_data_entry - offset);
        }
        //put key
        memcpy((char*)page + offset, newChildEntry, required_space);

        //update free space
        free_space = free_space - required_space;
        memcpy((char*)page + PAGE_SIZE - 3*sizeof (unsigned ), &free_space, sizeof (unsigned ));

        //update numkeys
        num_of_keys++;
        memcpy((char*)page + PAGE_SIZE - 2*sizeof (unsigned ), &num_of_keys, sizeof (unsigned ));
        return 0;
    }

    RC IndexManager::find_offset_for_putting_key_in_indexNode(void* &page, AttrType keyType, void* &newChildEntry, int& num_of_keys, int &required_space, int &keys_inspected, int &offset){
        if(keyType == TypeInt){
            required_space = 2*sizeof (unsigned );
            int key_to_put = *(int*)(newChildEntry);
            while(keys_inspected < num_of_keys){
                int curr_key_under_inspection = *(int*)((char*)page + offset + sizeof (unsigned ));
                if(key_to_put < curr_key_under_inspection){
                    break;
                }
                keys_inspected++;
                offset += 2*sizeof (unsigned );
            }
        }else if(keyType == TypeReal){
            required_space = sizeof (float) + sizeof (unsigned );
            float key_to_put = *(float*)newChildEntry;
            while(keys_inspected < num_of_keys){
                float curr_key_under_inspection = *(float*)((char*)page + offset + sizeof (unsigned ));
                if(key_to_put < curr_key_under_inspection){
                    break;
                }
                keys_inspected++;
                offset =  offset + sizeof (unsigned ) + sizeof (float);
            }
        }else{
            int key_to_put_len = *(int*)newChildEntry;
            char key_to_put[key_to_put_len + 1];
            memcpy(&key_to_put, (char*)newChildEntry + sizeof (unsigned ), key_to_put_len);
            key_to_put[key_to_put_len] = '\0';
            required_space = sizeof (unsigned ) + key_to_put_len + sizeof (unsigned );
            while(keys_inspected < num_of_keys){
                int len_curr_key_under_inspection = *(int*)((char*)page + offset + sizeof (unsigned ));
                char curr_key_under_inspection[len_curr_key_under_inspection + 1];
                memcpy(&curr_key_under_inspection, (char*)page + offset + 2*sizeof (unsigned ), len_curr_key_under_inspection);
                curr_key_under_inspection[len_curr_key_under_inspection] = '\0';
                if(strcmp(key_to_put , curr_key_under_inspection) < 0){
                    break;
                }
                keys_inspected++;
                //pointer + 4 byte for varchar len + varchar len
                offset += sizeof (unsigned ) + sizeof (unsigned ) + len_curr_key_under_inspection;
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
        }else if(keyType == TypeReal){
            memcpy((char*)page + offset, key, sizeof (float ));
            offset += sizeof (float );
        }else{
            int key_len = *(int*)(key);
            memcpy((char*)page + offset, key, sizeof (unsigned ) + key_len);
            offset += sizeof (unsigned ) + key_len;
        }

        memcpy((char*)page + offset, &rid.pageNum, sizeof (unsigned ));
        offset += sizeof (unsigned );
        memcpy((char*)page + offset, &rid.slotNum, sizeof (unsigned ));

        //update free space
        free_space = free_space - required_space;
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

    bool IndexManager::hasSpaceIndexNode(void* &page, AttrType keyType, void* &newChildEntry){
        int free_space = *(int*)((char*)page + PAGE_SIZE - 3*sizeof (unsigned ));
        //space for 1 pointer and the key
        int required_space = 0;

        if(keyType == TypeInt){
            // 1 for a pointer, one for key
            required_space = 2*sizeof (unsigned );
        }else if(keyType == TypeReal){
            required_space = sizeof (unsigned ) + sizeof (float );
        }else{
            int len_varchar = *(int*)(newChildEntry);
            required_space = 2*sizeof (unsigned ) + len_varchar;
        }

        if(required_space <= free_space) return true;
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

    bool IndexManager::isInternalNode(void* &page) const{
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

    int IndexManager::appendNewIndexPage(IXFileHandle &ixFileHandle, AttrType keyType, const void *key){
        void* page = malloc(PAGE_SIZE);
        memset(page, 0, PAGE_SIZE);
        int numPages = ixFileHandle.getNumberOfPages();
        int key_len = get_length_of_key(keyType, key);
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

    int IndexManager::get_root_page_index(IXFileHandle ixFileHandle) const{
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
        int root_page_index = get_root_page_index(ixFileHandle);
        if(root_page_index == -1){
            out << "{}";
            return 0;
        }
        printIndexNode(ixFileHandle, attribute.type, out, root_page_index);
        return 0;
    }
    //print format : ["A:[(1,1),(1,2)]","B:[(2,1),(2,2)]"]
    RC IndexManager::printLeafNode (void* &page, AttrType attrType, std::ostream &out) const{
        out << "[";
        int num_keys = *(int*)((char*)page + PAGE_SIZE - 2*sizeof (unsigned ));
        if(num_keys == 0){
            out << "]";
            return 0;
        }
        if(attrType == TypeInt){
            std::map<int, std::vector<Entry>> map;
            int i = 0;
            int offset = 0;
            while(i < num_keys){
                i++;
                int key  = *(int*)((char*)page + offset);
                offset += sizeof (unsigned );

                int pageNum = *(int*)((char*)page + offset);
                offset += sizeof (unsigned );
                int slotNum = *(int*)((char*)page + offset);
                offset += sizeof (unsigned );

                Entry entry(pageNum, slotNum);
                bool key_exists = false;


                if(map.find(key) != map.end()){
                    key_exists = true;
                }
                if(key_exists){
                    std::vector<Entry> v = map[key];
                    v.push_back(entry);
                    map[key] = v;
                }else{
                    std::vector<Entry> v;
                    v.push_back(entry);
                    map[key] = v;
                }
            }
            int map_size = map.size();
            int processed = 0;
            for (auto const &pair: map) {
                processed++;
                out << "\"" << pair.first << ":["; //["A:[
                std::vector<Entry> val = pair.second;
                for(int l = 0; l < val.size();l++){
                    out << "(" << val.at(l).pageNum << "," << val.at(l).slotNum << ")";  //["A:[(1,1)

                    if(l != val.size() -1){
                        out << ","; // ["A:[(1,1),(1,2)
                    }
                }
                out << "]" << "\"";
                if(processed != map.size() -1){
                    out << ",";  // ["A:[(1,1),(1,2)]","B:[(2,1),(2,2)]"
                }
            }

        }else if(attrType == TypeReal){
            std::map<float, std::vector<Entry>> map;
            int i = 0;
            int offset = 0;
            while(i < num_keys){
                i++;
                float key  = *(float*)((char*)page + offset);
                offset += sizeof (float);

                int pageNum = *(int*)((char*)page + offset);
                offset += sizeof (unsigned );
                int slotNum = *(int*)((char*)page + offset);
                offset += sizeof (unsigned );

                Entry entry(pageNum, slotNum);
                bool key_exists = false;

                if(map.find(key) != map.end()){
                    key_exists = true;
                }

                if(key_exists){
                    std::vector<Entry> v = map[key];
                    v.push_back(entry);
                    map[key] = v;
                }else{
                    std::vector<Entry> v;
                    v.push_back(entry);
                    map[key] = v;
                }
            }
            int map_size = map.size();
            int processed = 0;
            for (auto const &pair: map){
                processed++;
                out << "\"" << pair.first << ":["; //["A:[
                std::vector<Entry> val = pair.second;
                for(int l = 0; l < val.size();l++){
                    out << "(" << val.at(l).pageNum << "," << val.at(l).slotNum << ")";  //["A:[(1,1)

                    if(l != val.size() -1){
                        out << ","; // ["A:[(1,1),(1,2)
                    }
                }
                out << "]" << "\"";
                if(processed != map.size() -1){
                    out << ",";  // ["A:[(1,1),(1,2)]","B:[(2,1),(2,2)]"
                }
            }
        }else{
            std::map<std::string, std::vector<Entry>> map;
            int i = 0;
            int offset = 0;
            while(i < num_keys){
                int len = *(int*)((char*)page + offset);
                offset += sizeof (unsigned );
                char key[len + 1];
                memcpy(key, (char*) page + offset, len * sizeof(char));
                offset += len * sizeof (char);
                key[len] = '\0';

                int pageNum = *(int*)((char*)page + offset);
                offset += sizeof (unsigned );
                int slotNum = *(int*)((char*)page + offset);
                offset += sizeof (unsigned );

                Entry entry(pageNum, slotNum);
                bool key_exists = false;

                if(map.find(key) != map.end()){
                    key_exists = true;
                }
                if(key_exists){
                    std::vector<Entry> v = map[key];
                    v.push_back(entry);
                    map[key] = v;
                }else{
                    std::vector<Entry> v;
                    v.push_back(entry);
                    map[key] = v;
                }
            }
            int map_size = map.size();
            int processed = 0;
            for (auto const &pair: map) {
                processed++;
                out << "\"" << pair.first << ":["; //["A:[
                std::vector<Entry> val = pair.second;
                for(int l = 0; l < val.size();l++){
                    out << "(" << val.at(l).pageNum << "," << val.at(l).slotNum << ")";  //["A:[(1,1)

                    if(l != val.size() -1){
                        out << ","; // ["A:[(1,1),(1,2)
                    }
                }
                out << "]" << "\"";
                if(processed != map.size() -1){
                    out << ",";  // ["A:[(1,1),(1,2)]","B:[(2,1),(2,2)]"
                }
            }
        }
        out << "]";
    }

    RC IndexManager::printIndexNode (IXFileHandle &ixFileHandle, AttrType attrType, std::ostream &out, int node_index) const{
        out << "{\"keys\":";
        void* page = malloc(PAGE_SIZE);
        ixFileHandle.readPage(node_index, page);
        bool is_internal = isInternalNode(page);
        if(!is_internal){
            printLeafNode(page, attrType, out);
            out << "}";
            return 0;
        }
        int num_keys = *(int*)((char*)page + PAGE_SIZE - 2*sizeof (unsigned ));
        //case handling for empty index node
        //not sure if would occur, depends on deletion
        if(num_keys == 0){
            out << "[]";
            out << ", \"children\": []";
            out << "}";
            return 0;
        }
        //else index_node has entries
        std::vector<PageNum> children;
        int offset = 0;
        for(int i = 0; i < num_keys; i++){
            if(i == 0) {
                out << "[";
            }
            int child = *(int*)((char*)page + offset);
            children.push_back(child);
            offset += sizeof (unsigned );
            if(attrType == TypeInt){
                int key = *(int*)((char*)page + offset);
                out << key;
                offset += sizeof (unsigned );
            }else if(attrType == TypeReal){
                float key = *(float*)((char*)page + offset);
                out << key;
                offset += sizeof (float );
            }else{
                int len = *(int*)((char*)page + offset);
                offset += sizeof (unsigned );
                char array[len + 1];
                memcpy(array, (char*) page + offset, len * sizeof(char));
                offset += len * sizeof (char);
                array[len] = '\0';
                out << array;
            }

            if( i < (num_keys - 1)){
                out << ",";
            }else{
                out << "],";
            }
        }
        //till now, we created {"keys":["C","G","M"],
        //push the last pointer page
        children.push_back(*(int*)((char*) page + offset));
        //Now create child
        out << "\"children\": [";
        for(int k = 0; k < children.size(); k++){
            printIndexNode(ixFileHandle, attrType, out, children.at(k));
            if(k != children.size() - 1){
                out << ",";
            }
        }
        out << "]" << "}";
    }
    RC IndexManager::preOrder(int node_index, IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out){
        void* page = malloc(PAGE_SIZE);
        memset(page, 0, PAGE_SIZE);
        ixFileHandle.readPage(node_index, page);
        bool is_internal = isInternalNode(page);


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