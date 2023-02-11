## Project 2 Report


### 1. Basic information
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222-winter23-MiraKabra
 - Student 1 UCI NetID: 69963587
 - Student 1 Name: Mira Kabra


### 2. Meta-data
- Show your meta-data design (Tables and Columns table) and information about each column.

#### Tables:

| Attribute Name    | Attribute type  | Attribute Length      | Attribute type                                                                                                                                                                                                                         |
|-------------------|-----------------|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table-id          | TypeInt         | 4                     | This stores the table-id of a table. The table-id for Tables table is 1 and for Columns table is 2. Any other table insertion starts after that                                                                                        |
| table-name        | TypeVarChar     | 50                    | This stores the table name of the table                                                                                                                                                                                                |
| file-name         | TypeVarChar     | 50                    | This stores the name of the binary file corresponding to the table where all insertion, seletion record etc happen.                                                                                                                    |
| system            | TypeInt         | 4                     | This acts a a boolean to tell whether this table is a system table or not. Some operations won't be permitted by the user if the table is a system table. eg. an user can't delete or insert in a Tables or Column table by themselves |


#### Columns:


| Attribute Name    | Attribute type  | Attribute Length      | Attribute type  |
|-------------------|-----------------|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table-id          | TypeInt         | 4                     | This stores the table-id of a table. The table-id for Tables table is 1 and for Columns table is 2. Any other table insertion starts after that                                                                                        |
| column-name        | TypeVarChar     | 50                    | This stores the attribute names of that table. First 8 rows are for Tables and Columns table attribute. The attribute 'system' is not inserted as that is for internal use and should not be visible to the user if they do a read operation.                                                                                                                                                                                             |
| column-type         | TypeInt    | 4                   | This stores the type of the corresponding attribute.                                                                                                                    |
| column-length           | TypeInt         | 4                     | This stores the max length of the corresponding attribute |
| column-position           | TypeInt         | 4                     | This stores the relative position of the attribute. It is especially useful for schema versioning. |

### 3. Internal Record Format (in case you have changed from P1, please re-enter here)
- Show your record format design.

Same for records other than internal record and tombstone pointers.

#### Regular record:
![alt text](https://github.com/UCI-Chenli-teaching/cs222-winter23-MiraKabra/blob/assignment-1/report/record%20format.png?raw=true)

#### Internal record:


#### Tombstone pointer:


- Describe how you store a null field.

Not changed


- Describe how you store a VarChar field.

Not changed


- Describe how your record design satisfies O(1) field access.

Not changed


### 4. Page Format (in case you have changed from P1, please re-enter here)
- Show your page format design.

Not changed


- Explain your slot directory design if applicable.

![alt text](https://github.com/UCI-Chenli-teaching/cs222-winter23-MiraKabra/blob/assignment-1/report/Page%20format.png?raw=true)


In the slot directory, after delete record, that corresponding slot's length and start address fields both gets assigned value -1. And if a new record is getting inserted in that page, it will populate that hole.

### 5. Page Management (in case you have changed from P1, please re-enter here)
- How many hidden pages are utilized in your design?

Not changed

- Show your hidden page(s) format design if applicable

Not changed

### 6. Describe the following operation logic.
- Delete a record

```angular2svg
- delete the record
- fill length of start address in the slots = -1 to mark it as hole
- shift other records to left
- Change the start address offset of the shifted records
- Update free space value
```

- Update a record
```angular2svg

To update a record: rec

- if (update length < curr length)
    - shift other records to left by (curr length of rec - update length of rec)
    - Change the start address offset of the shifted records
    - Update the record rec
    - Update free space value
- else
    - if enough free space available on the same page:
        - shift other records to left by (update length of rec - curr length of rec)
        - Change the start address offset of the shifted records
        - Update the record rec
        - Update free space value
    - else
        - Find a page with enough free space 
        - convert the curr rid of the rec to become a tombstone to store the found slot number and page number
        - insert the record there with it's original rid data attached to it's tail
```


- Scan on normal records

```angular2svg
In getNextRecord:
- Keep going through records one by one
    - if a record meets the condition
        - store the rid and data
        - return 
    - else if it hits the end of file
        - return RBFM_EOF
```


- Scan on deleted records
```angular2svg
In getNextRecord:
- Keep going through records one by one
    - if it's a hole
        - skip
    - if a record meets the condition
        - store the rid and data
        - return
    - else if it hits the end of file
        - return RBFM_EOF

```

- Scan on updated records
```angular2svg
In getNextRecord:
- Keep going through records one by one
    - if it's an internal id
        - if a record meets the condition
            - extract the original rid from the data and store.
            - store the data
            - return
    - if it's a tombstone
        - skip
    - if normal record
        - if a record meets the condition
            - store the rid and data
            - return
    - else if it hits the end of file
        - return RBFM_EOF

```

