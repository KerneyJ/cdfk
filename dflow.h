#ifndef __DFLOW__
#define __DFLOW__

#include <Python.h>
#include <limits.h>

#define TABLE_INC 10000 // add room for 10k task whenever we need to increase tablesize

/* 
 * Based on States enum in parsl/dataflow/states.py found at
 * https://github.com/Parsl/parsl/blob/master/parsl/dataflow/states.py
 */
enum state{
   unsched=-1,
   pending=0,
   running=2,
   exec_done=3,
   failed=4,
   dep_fail=5,
   launched=7,
   fail_retryable=8,
   memo_done=9,
   joining=10,
   running_ended=11,
};

// task dependency struct
struct task
{
    unsigned long id;
	int status;
    unsigned long* depends; // list of indexes in tasktable  
    unsigned long depcount;
	char* exec_label; // executor is label so can be string
	char* func_name;
	int time_invoked; // unix timestamp
    int join; // 0: is not a join app; 1 is a join app;

	PyObject* future;
	PyObject* executor;
	PyObject* func;
	PyObject* args;
	PyObject* kwargs;
};

/*
 * For now we will support Parsl Apps that
 * do not require datafutures. In the future
 * if this experiment continues then we will
 * use this struct to keep track of data futures
 * the task struct will have a data struct buffer
 */
struct data
{
    unsigned long id;
    enum state status;
};

PyObject* method(PyObject*, PyObject*); // test


void init_tasktable(unsigned long); // allocate initial amount of memory for table
int resize_tasktable(unsigned long); //  change the amount of memory in table
int increment_tasktable(void); // will try to increase table size by TABLE_INC
int appendtask(char*, char*, int, int, PyObject*, PyObject*, PyObject*, PyObject*, PyObject*); // add a task to the dfk

PyObject* init_dfk(PyObject*, PyObject*);
PyObject* dest_dfk(PyObject*);
PyObject* info_dfk(PyObject*);
PyObject* submit(PyObject*, PyObject*);

#endif
