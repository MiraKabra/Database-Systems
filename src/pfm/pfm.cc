#include "src/include/pfm.h"
#include <stdio.h>
#include <cmath>

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {

        FILE* pFile;
        pFile = fopen(fileName.c_str(), "r");

        //If file does not exist, it should have returned null when trying to
        //open in read mode. Not null means file existed, hence throw an error
        if(pFile != nullptr){
            cout << "File already existed";
            return -1;
        }

        //Finally file does not exist, do open a file in write mode to create it
        //Then close the file
        pFile = fopen(fileName.c_str(), "w");
        fclose(pFile);
        return 0;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {

        int return_val = remove(fileName.c_str());
        //if not successful, the return_val wil be nonzero
        return return_val;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        // If the fileHandle already had a file assigned to it, raise error
        if(fileHandle.getFile() != nullptr) return -1;

        FILE* pFile;
        pFile = fopen(fileName.c_str(), "r");
        // This file does not exist
        if(pFile == nullptr){
            return -1;
        }
        //Assign the file to fileHandle. File is in Open state now and in read mode
        fileHandle.setFile(pFile);
        return 0;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        //If no file was bound to the fileHandle, raise error
        if(fileHandle.getFile() == nullptr) return -1;

        //Flush the file data to disk
        FILE* pFile = fileHandle.getFile();

        //close the file
        //date will be flushed to disk automatically when file is closed
        int return_val = fclose(pFile);
        //After closing the file, set it to null in the fileHandle
        //If not closed properly, EOF(-1) is returned
        if(return_val != 0) return return_val;
        fileHandle.setFile(nullptr);
        return 0;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() = default;

    //Getter function for associated file
    FILE* FileHandle::getFile(){
        return file;
    }
    //Setter function for associated file
    void FileHandle::setFile(FILE* file){
        this->file = file;
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        // To access the pages of a file, a client first creates an instance of this class and
        // passes it to the PagedFileManager::openFile method described above.

        FILE* pFile = this->getFile();
        //check if the file exists
        if(pFile == nullptr) return -1;

        //check if the page exists
        int numPages = getNumberOfPages();
        if(pageNum > numPages) return -1;

        //The page may not be completely full. That case is not handled.
        //Here loading the entire page into memory

        //Set position to the start of the pageNum
        fseek(pFile, pageNum*PAGE_SIZE, SEEK_SET);
        //read page
        size_t result = fread(data, PAGE_SIZE, 1, pFile);
        //Increase readPageCount
        readPageCounter++;
        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {

        FILE* pFile = this->getFile();
        //check if the file exists
        if(pFile == nullptr) return -1;

        //check if the page exists
        int numPages = getNumberOfPages();
        if(pageNum > numPages) return -1;
        /* write the data from start of the page.
         * Calculate start Address.
         * Add 1 due to hidden page*/
        long int offset = (numPages + 1)*PAGE_SIZE;
        fseek(pFile, offset, SEEK_SET);
        fwrite(data, sizeof(data), 1, pFile);

        //Increase write page counter
        writePageCounter++;
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        FILE* pFile = this->getFile();

        //check if the file exists
        if(pFile == nullptr) return -1;

        //check if file is empty i.e. appending for the first time
        int numPages = getNumberOfPages();
        /*If appending for the first time, then add a hidden page also.
         * and then add a next page with the data.
         * If not appending for the first time, then first get the number of pages
         * already there to calculate inserting address of the data*/
        if(numPages == 0){
            //First page is as hidden page. Write after that
            fseek(pFile, PAGE_SIZE, SEEK_SET);
            fwrite(data, sizeof(data), 1, pFile);

        }else{
            // 1 is added to take hidden page in account
            long int start_address = (numPages + 1)*PAGE_SIZE;
            fseek(pFile, start_address, SEEK_SET);
            fwrite(data, sizeof(data), 1, pFile);

        }
        //Increase append count
        appendPageCounter++;
        //Update the new 'number of pages' in hidden file
        numPages++;
        fseek(pFile, 0, SEEK_SET);
        fwrite(&numPages, 4, 1, pFile);

        //If appending for the first time, then add a hidden page also
        return -1;
    }

    unsigned FileHandle::getNumberOfPages() {
        FILE* pFile = this->getFile();
        fseek(pFile, 0, SEEK_END);
        long int size_of_file = ftell(pFile);
        //No hidden page case
        if(size_of_file == 0) return 0;
        //If code has come till here, it means number of pages are more than zero
        //So, the first page is a hidden page
        //come to beginning of file
        fseek(pFile, 0, SEEK_SET);
        //read first four bytes where number of page value exists
        int numOfPages;

        fread(&numOfPages, 4, 1, pFile);

        return numOfPages;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return 0;
    }

    //Return size of the file
    long int FileHandle::sizeOfFile(FILE* pFile){
        fseek(pFile, 0, SEEK_END);
        long int size = ftell(pFile);
        //Set position indicator to the beginning of the file
        rewind(pFile);
        return size;
    }

} // namespace PeterDB