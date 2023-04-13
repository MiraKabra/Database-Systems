## Debugger and Valgrind Report

### 1. Basic information
 - Team #: Mira Kabra
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222-winter23-MiraKabra
 - Student 1 UCI NetID: mkabra1 (69963587)
 - Student 1 Name: Mira Kabra
 - Student 2 UCI NetID (if applicable):
 - Student 2 Name (if applicable):


### 2. Using a Debugger
- Describe how you use a debugger (gdb, or lldb, or CLion debugger) to debug your code and show screenshots. 
For example, using breakpoints, step in/step out/step over, evaluate expressions, etc.

I used clion debugger. Screenshots are attached below:

![alt text](https://github.com/MiraKabra/Database-Systems/blob/assignment-1/report/debugger/debug_file1.png?raw=true)

![alt text](https://github.com/MiraKabra/Database-Systems/blob/assignment-1/report/debugger/debug_file2.png?raw=true)

![alt text](https://github.com/MiraKabra/Database-Systems/blob/assignment-1/report/debugger/debug_file3.png?raw=true)

![alt text](https://github.com/MiraKabra/Database-Systems/blob/assignment-1/report/debugger/debug_file4.png?raw=true)


### 3. Using Valgrind
- Describe how you use Valgrind to detect memory leaks and other problems in your code and show screenshot of the Valgrind report.

I used valgrind to check how much memory was allocated, got freed.
I also checked for uninitialized error, mismatch error etc. 

In pfm file: No memory was leaked. And all the tests passed. Screenshot of tests passed is attached.

```angular2svg
HEAP SUMMARY:
in use at exit: 0 bytes in 0 blocks
total heap usage: 1,048 allocs, 1,048 frees, 3,021,941 bytes allocated

All heap blocks were freed -- no leaks are possible

ERROR SUMMARY: 0 errors from 0 contexts (suppressed: 0 from 0)
```


In rbfm file: No memory was leaked. And all the tests passed. Screenshot of tests passed is attached.

```angular2svg
HEAP SUMMARY:
in use at exit: 0 bytes in 0 blocks
total heap usage: 84,796 allocs, 84,796 frees, 159,406,049 bytes allocated

All heap blocks were freed -- no leaks are possible

For lists of detected and suppressed errors, rerun with: -s
ERROR SUMMARY: 0 errors from 0 contexts (suppressed: 0 from 0)

```

There was one error coming from the rbfm_test_utils.h file because an object created by new was getting deleted by free, which was a mismatch. I changed it to malloc.

![alt text](https://github.com/MiraKabra/Database-Systems/blob/assignment-1/report/valgrind/pfm.png?raw=true)

![alt text](https://github.com/MiraKabra/Database-Systems/blob/assignment-1/report/valgrind/pfm2.png?raw=true)

![alt text](https://github.com/MiraKabra/Database-Systems/blob/assignment-1/report/valgrind/rbfm.png?raw=true)

![alt text](https://github.com/MiraKabra/Database-Systems/blob/assignment-1/report/valgrind/rbfm2.png?raw=true)
