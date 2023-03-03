## Project 3 Report


### 1. Basic information
 - Team #:
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222-winter23-MiraKabra
 - Student  UCI NetID: 69963587
 - Student  Name: Mira Kabra


### 2. Meta-data page in an index file
- Show your meta-data page of an index design if you have any.
  ![alt text](https://github.com/UCI-Chenli-teaching/cs222-winter23-MiraKabra/blob/assignment-3-extra-feature/report/metadata_page.png?raw=true)

### 3. Index Entry Format
- Show your index entry design (structure). 

  - entries on internal nodes:  
  ![alt text](https://github.com/UCI-Chenli-teaching/cs222-winter23-MiraKabra/blob/assignment-3-extra-feature/report/index_entry.png?raw=true)
  
  - entries on leaf nodes:

  ![alt text](https://github.com/UCI-Chenli-teaching/cs222-winter23-MiraKabra/blob/assignment-3-extra-feature/report/leaf_entry.png?raw=true)

### 4. Page Format
- Show your internal-page (non-leaf node) design.

![alt text](https://github.com/UCI-Chenli-teaching/cs222-winter23-MiraKabra/blob/assignment-3-extra-feature/report/internal_node_page.png?raw=true)

- Show your leaf-page (leaf node) design.

![alt text](https://github.com/UCI-Chenli-teaching/cs222-winter23-MiraKabra/blob/assignment-3-extra-feature/report/leaf_node_page.png?raw=true)

### 5. Describe the following operation logic.
- Split

```angular2svg
- if leaf node
    - create a temp page of 2*PAGE_SIZE
    - copy all the data of the page to temp_page
    - insert the entry in the temp_page maintaining ascending order of the key
    - Find mid point in the temp_page such that d data resides on old page and d+1 in new page
    - copy the d+1 data to a new page, keep only d entries in old page
    - store the smallest key of new page in newChildEntry
    - if leaf node was a root node
        - create an internal node with this newChildEntry
        - Make the two leaf node child of this internal node
        - update the root pagenum in dummy page

- if internal node
    - create a temp page of 2*PAGE_SIZE
    - copy all the data of the page to temp_page
    - insert the entry in the temp_page maintaining ascending order of the key
    - Find mid point in the temp_page such that leftmost d entries resides on old page, copy the next entry in newChildEntry and rightmost d entries in new page
    - if internal node was a root node
        - create an internal node with this newChildEntry
        - Make the two internal node child of this internal node
        - update the root pagenum in dummy page
```

- Rotation (if applicable)
N/A


- Merge/non-lazy deletion (if applicable)

Merge/non-lzy deletion has been implemented by redistributing entries when page becomes half ful and sibling has extra entries to spare. If sibling does not have extra entries, nodes get merged. When root has two child and they get merged, the current root no longer remains the root, and the merged node becomes the new root. This info gets updated in the dummy root 
page as well.

- Duplicate key span in a page

The duplicate keys are kept continuously. When split page operation occurs, rather than splitting the page half/half, splitting occurs after streak of a duplicate entry ends. Hence, if a page contains duplicate entry, most of the time splitting will not produce two pages of holding similar amount of entries.

- Duplicate key span multiple pages (if applicable)
This case is covered treating the entries like other entries, but multiple leaf pages will contain same key in this case and such the leaf pages will be sequential. In case of search operation, we go to the left most page of the searched key and sequentially access next pages also to look for the key if streak of keys continues.


### 6. Implementation Detail
- Have you added your own module or source file (.cc or .h)? 
  Clearly list the changes on files and CMakeLists.txt, if any.
No
