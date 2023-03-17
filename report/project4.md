## Project 4 Report


### 1. Basic information
- Name: Mira Kabra
- Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222-winter23-MiraKabra
- Student UCI NetID: 69963587


### 2. Catalog information about Index
- Show your catalog information about an index (tables, columns).

One row for the "Indexes" table has been included in the "Tables" table.
#### Indexes Table:

| table_id | attributeName  |  index_filename                                                                                                                                                                                                                         |
|----------|-----------------|-----------------------|
| 13       | age | file-age.idx|





### 3. Filter
- Describe how your filter works (especially, how you check the condition.)

The condition attribute gets extracted from each tuple. After that this is compared with 'cond.rhsValue.data' based on the operator given. If it satisfies the condition, this tuple is returned as the next tuple from the Filter condition.


### 4. Project
- Describe how your project works.

For every tuple that it gets from the input iterator, it creates a data with the asked attributes in the asked order and returns it. getNextTuple on filter will be successful till the input iterator has reached th end of file.

### 5. Block Nested Loop Join
- Describe how your block nested loop join works (especially, how you manage the given buffers.)

Here, for the outer iterator, tuples are loaded unit total size of tuple is less than equal to numPage*PAGE_SIZE. For the inner table, tuples are loaded unit total size of tuple is less than equal to PAGE_SIZE. Hash table is created for both based on the join key. After that join operation is performed on this two.
Once, all the joined tuples go exhausted by the BNLJoin nextTuple call, more tuples PAGE_SIZE amount is loaded from inner table and again join operation is performed. 

Once, the inner table iterator reaches the end, more tuples of numPages*PAGE_SIZE amount is loaded from the outer table. The iterator on the inner table is made point to the beginning and the same process takes place again.

This continues as long as join operation has considered all the tuples from the left as well as right table.


### 6. Index Nested Loop Join
- Describe how your index nested loop join works.
```angular2svg
- For each tuple of outer table (left table):
   - read the join key attribute from outer table tuple
   - Based on the condition provided, create an index scan on the index tree of the right table
   - Get all the tuples from rid that got extracted from the index tree
    - join the tuples and store in a vector
    - return tuple one by one from vector and increment index pointer
    - when the joined tuples got exhaused, repeat the process for next tuple of outer table
```


### 7. Grace Hash Join (If you have implemented this feature)
- Describe how your grace hash join works (especially, in-memory structure).



### 8. Aggregation
- Describe how your basic aggregation works.
```angular2svg
- create a variable called result
- till there is a tuple in the iterator
    - get value of aggregation attribute
    - based on the aggregate operation, compute the new value for the result
- create a data with a bitmap and result
- return this data
```

- Describe how your group-based aggregation works. (If you have implemented this feature)
```angular2svg
- create a vector[int] and map[int, int]
- create curr_index of vector as -1
- till there is a tuple in the iterator
    - get value of group attribute
    - get value of aggregation attribute
    - based on the aggregate operation, compute the new value for the result
    - store the group attribute in vector
    - store the group attribute, aggregation value pair in the map
- for each index in vector:
    -increment curr_index
    - if curr_index reached the end, return -1
    - create a data with a bitmap, group val and aggr val
    - return this data
```



### 9. Texera Data Analytics
- Please find the video of the analysis: https://youtu.be/WFA8Bo55WA8