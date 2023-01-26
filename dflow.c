#include "dflow.h"

/*
 * Notes, FIXMEs and TODOs
 * - TODO A lot of functions return NULL in case
 *   of error, need to change this to set error
 *   using PyErr_SetString and use error codes from
 *   https://docs.python.org/3/c-api/exceptions.html#standard-exceptions
 */

struct task* tasktable = NULL; // dag represented as table of task structs
unsigned long tablesize; // number of tasks table can store
unsigned long taskcount; // number of tasks created

PyObject* method(PyObject* self, PyObject* args){
    // int a, b;
    PyObject* obj;
    if(!PyArg_ParseTuple(args, "O", &obj))
        return NULL;

    return Py_None; //return Py_BuildValue("i", a + b);
}

int init_tasktable(unsigned long numtasks){
    tasktable = (struct task*)PyMem_RawMalloc(sizeof(struct task) * numtasks);
    if(tasktable == NULL)
        return -1;
    tablesize = numtasks;
    taskcount = 0;
    return 0;
}

int resize_tasktable(unsigned long numtasks){
    if(numtasks > ULONG_MAX) // check if size is too big
        return -1;

    if(!tasktable) // check if task table has been initialized
        return -1;

    if((tasktable = (struct task*)PyMem_RawRealloc(tasktable, numtasks * sizeof(struct task*))) == NULL)
        return -1;

    tablesize = numtasks;
    return 0;
}

int increment_tasktable(){
    if(tablesize + TABLE_INC > ULONG_MAX)
        return -1;
    return resize_tasktable(tablesize + TABLE_INC);
}

/*
 * In future create function for deleting task to
 * conserve space in the task table. Right now the
 * goal if to implement something super simple and
 * functional so task will not be deleted we will
 * just add new task in the next unused spot
 */
int appendtask(char* exec_label, char* func_name, double time_invoked, int join, PyObject* future, PyObject* executor, PyObject* func, PyObject* args, PyObject* kwargs){
    // check if the table is large enough
    if(taskcount == tablesize)
        if(increment_tasktable() < 0)
            return -1;

    struct task* task = &tasktable[taskcount];
    task->id = taskcount;
    taskcount++;
    task->status = unsched;
    task->depends = NULL;
    task->depcount = 0;

    task->exec_label = exec_label;
    task->func_name = func_name;
    task->time_invoked = time_invoked;
    task->join = join;

    task->future = future;
    task->executor = executor;
    task->func = func;
    task->args = args;
    task->kwargs = kwargs;

    return 0;
}

PyObject* init_dfk(PyObject* self, PyObject* args){
    unsigned long numtasks;
    if(!PyArg_ParseTuple(args, "k", &numtasks))
        return NULL;

    if(init_tasktable(numtasks) < 0)
        return NULL;

    return Py_None;
}

/*
 * It is to my current belief that the memory
 * consumed by the task struct(pointers) is managed
 * by the python interpreter but if memory becomes an
 * issue then we should make sure that this is the case
 */
PyObject* dest_dfk(PyObject* self){
    if(tasktable != NULL)
        PyMem_RawFree(tasktable);

    tablesize = 0;
    taskcount = 0;

    return Py_None;
}

PyObject* info_dfk(PyObject* self){
    return PyUnicode_FromFormat("DFK Info -> Tasktable pointer: %p; Task table size: %i; Task count: %i;", tasktable, tablesize, taskcount);
}

PyObject* info_task(PyObject* self, PyObject* args){
    unsigned long id;

    if(!PyArg_ParseTuple(args, "k", &id))
        return NULL;

    if(tasktable == NULL) // TODO throw error here
        return PyUnicode_FromFormat("DFK Uninitialized");

    if(id >= taskcount) // TODO throw error here
        return PyUnicode_FromFormat("Task unallocated");

    struct task task = tasktable[id];

    return PyUnicode_FromFormat("Task %lu -> state: %i; depcount: %lu; exec_label: %s; func_name: %s; time invoked: %i; join: %i", // TODO find how to print float
                                task.id, task.status, task.depcount, task.exec_label, task.func_name, task.time_invoked, task.join);
}

/*
 * TODO When freeing a task will need to decrement the refernce counts
 * of the python objects taken as a argument
 */
PyObject* submit(PyObject* self, PyObject* args){
    char* exec_label,* func_name;
    int join;
    double time_invoked;
    PyObject* future,* executor,* func,* fargs,* fkwargs;

    if(!PyArg_ParseTuple(args, "ssdpOOOOO", &exec_label, &func_name, &time_invoked, &join, &future, &executor, &func, &fargs, &fkwargs))
        return NULL;

    if(appendtask(exec_label, func_name, time_invoked, join, future, executor, func, fargs, fkwargs) < 0)
        return NULL;

    return Py_None;
}
