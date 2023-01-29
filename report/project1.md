## Project 1 Report


### 1. Basic information
 - Team #: Mira Kabra
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222-winter23-MiraKabra
 - Student 1 UCI NetID: mkabra1 (69963587)
 - Student 1 Name: Mira Kabra
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):


### 2. Internal Record Format

1. The record format will have the following fields from start to end:
 - First field : N (Number of non null values) [Stored in 4 bytes]
 - Second field : A bitmap of 4 bytes indicating if any column value is null or not. The leftmost bit will be indicator the first column and so on. If any column value is null, corresponding bit will be set to 1, else 0.
 - Next N fields of 4 byte each: Each of these will store the end address of the corresponding non-null value
 - Next N fields for each of the value. eg. for varchar of 8 bytes long, only 8 byte will be allocated for the varchar. For RealType and IntType, always 4 bytes will be indicated.

![alt text](https://github.com/UCI-Chenli-teaching/cs222-winter23-MiraKabra/blob/assignment-1/report/record%20format.png?raw=true)


2. Describe how you store a null field.
The bitmap in the data is used to see if any field is null or not. If a field is null, then the corresponding bit in the record bitmap is also set to 1 and no info for that column is stored in the record.

3. Describe how you store a VarChar field.
The stored end address of varchar is used to calculate the length of varchar. The N value stored at the beginning of the record makes it easy to get the start address. eg. if there is a varchar of 8 bytes, then only 8 bytes is used to store the varchar.


4. Describe how your record design satisfies O(1) field access.

For getting any field, we need to access only the previous slot of mini directory to get the start address of this field and also access the mini directory slot of this field to get the end address of this field. Thus we can get the length of the field by simply subtracting the start address from end addess. Thus only 2 access is need to get the field value making it a O(1) operation.

### 3. Page Format
- Show your page format design.
  ![alt text](https://github.com/UCI-Chenli-teaching/cs222-winter23-MiraKabra/blob/assignment-1/report/Page%20format.png?raw=true)

- Explain your slot directory design if applicable.

a. The directory slot from right to left in the bottom of the page is as follows: [Page size is 4096 bytes]

b. Right most is a 4 byte slots that stores free space in the page F.

c. On left of that is N (number of records in the page) stored in 4 bytes

d. On left of that there are two slots for each record. First 4 byte slot stores length of record and next 4 byte store start address of record. 


### 4. Page Management
- Show your algorithm of finding next available-space page when inserting a record.
```angular2svg
If numPage is zero
    - Append a new page with data
else:
    required_space = recordSize + 2*4 bytes for metadata
    - if (freespace in last page is >= requiredSize)
        - write to it
    - else
        - start comparing freespace from starting page
        - if found any suitable page
            - return that page index and write to it
        -else
            - append a new page and write to it
```


- How many hidden pages are utilized in your design?

Only one hidden page is used in the design


- Show your hidden page(s) format design if applicable

Hidden page stores: first 4 byte for Number Of pages (excluding hidden page), next 4 byte for readcounter. next 4 byte for writecounter, next 4 byte for appendcounter.


### 5. Implementation Detail
- Other implementation details goes here.
There is an encoder and decoder implemented. The encoder encodes the data into the record format before writing to the file.
The decoder decodes the record into the desired data format after reading. Printrecord function is implemented for debugging purpose.
