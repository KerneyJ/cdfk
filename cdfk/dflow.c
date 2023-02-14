/*
 * Notes, FIXMEs and TODOs
 * - TODO A lot of functions return NULL in case
 *   of error, need to change this to set error
 *   using PyErr_SetString and use error codes from
 *   https://docs.python.org/3/c-api/exceptions.html#standard-exceptions
 * - TODO have multiple queues, a queue for waitng & pending
 * - TODO maybe split the executor off into another process and
 *   comunicate with it via a zmq pipe
 * - TODO Change all the python bindings to static, this is advised by
 *   the documentation refernced in this stack overflow post
 *   https://stackoverflow.com/questions/18745319/why-function-is-static-in-python
 */
#include <Python.h>
#include <limits.h>

#define TABLE_INC 10000

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
struct task{
    unsigned long id;
    enum state status;
    unsigned long* depends;
    unsigned long depcount;
    char* exec_label;
    char* func_name;
    double time_invoked;
    int join;

    PyObject* future;
    PyObject* executor;
    PyObject* func;
    PyObject* args;
    PyObject* kwargs;
};

static int init_tasktable(unsigned long); // allocate initial amont of memory for table
static int resize_tasktable(unsigned long); // change amount of memory in table
static int increment_tasktable(void); // will try to increase table size by TABLE_INC
static int appendtask(char*, char*, double, int, PyObject*, PyObject*, PyObject*, PyObject*, PyObject*); // add a task to the dfk

static PyObject* init_dfk(PyObject*, PyObject*);
static PyObject* dest_dfk(PyObject*);
static PyObject* info_dfk(PyObject*);
static PyObject* add_executor_dfk(PyObject*, PyObject*);
static PyObject* info_task(PyObject*, PyObject*);
static PyObject* submit(PyObject*, PyObject*);

struct task* tasktable = NULL; // dag represented as table of task structs
PyObject** executors = NULL; // Array of executor PyObject* TODO Will need to check the reference count of these objects before picking them and launching
unsigned int executorssize = 0;
unsigned int executorcount= 0;
unsigned long tablesize; // number of tasks table can store
unsigned long taskcount; // number of tasks created

/*
 * In order to invoke object methods we must provide
 * PyObject_CallMethodObjArgs a PyObject that stores
 * a string of the method name so the following PyObjects
 * are will do so and they will be set in the dfk
 * initialization phase. Likely need to decrement ref
 * counter in destroy dfk phase
 */

PyObject* pystr_submit = NULL;

static int init_tasktable(unsigned long numtasks){
    tasktable = (struct task*)PyMem_RawMalloc(sizeof(struct task) * numtasks);
    if(tasktable == NULL)
        return -1;
    tablesize = numtasks;
    taskcount = 0;
    return 0;
}

static int resize_tasktable(unsigned long numtasks){
    if(numtasks > ULONG_MAX) // check if size is too big
        return -1;

    if(!tasktable) // check if task table has been initialized
        return -1;

    if((tasktable = (struct task*)PyMem_RawRealloc(tasktable, numtasks * sizeof(struct task*))) == NULL)
        return -1;

    tablesize = numtasks;
    return 0;
}

static int increment_tasktable(){
    if(tablesize + TABLE_INC > ULONG_MAX)
        return -1;
    return resize_tasktable(tablesize + TABLE_INC);
}

/*
 * In future create function for deletinrg task to
 * conserve space in the task table. Right now the
 * goal if to implement something super simple and
 * functional so task will not be deleted we will
 * just add new task in the next unused spot
 */
static int appendtask(char* exec_label, char* func_name, double time_invoked, int join, PyObject* future, PyObject* executor, PyObject* func, PyObject* args, PyObject* kwargs){
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

static PyObject* init_dfk(PyObject* self, PyObject* args){
    unsigned long numtasks;
    if(!PyArg_ParseTuple(args, "k", &numtasks))
        return NULL;

    if(init_tasktable(numtasks) < 0)
        return NULL;

    executorssize = 2; // default size of executor array to 2(1 for internal and 1 for external)
    executors = (PyObject**)PyMem_RawMalloc(executorssize * sizeof(PyObject*));

    pystr_submit = Py_BuildValue("s", "submit");
    return Py_None;
}

/*
 * It is to my current belief that the memory
 * consumed by the task struct(pointers) is managed
 * by the python interpreter but if memory becomes an
 * issue then we should make sure that this is the case
 */
static PyObject* dest_dfk(PyObject* self){
    if(tasktable != NULL)
        PyMem_RawFree(tasktable);

    tablesize = 0;
    taskcount = 0;

    return Py_None;
}

static PyObject* info_dfk(PyObject* self){
    return PyUnicode_FromFormat("DFK Info -> Tasktable pointer: %p; Task table size: %i; Task count: %i;", tasktable, tablesize, taskcount);
}


static PyObject* add_executor_dfk(PyObject* self, PyObject* args){
    PyObject* executor;
    if(!PyArg_ParseTuple(args, "O", &executor)) // TODO type check executor object to make sure it isn't a list, tuple, or other iterable
        return NULL;
    if(executorcount == executorsize){ // executor count should never be greater than executorsize because it should only ever be incremented by 1
        if((executors = PyMem_RawRealloc(executors, (executorsize+1) * sizeof(PyObject*))) == NULL)
            return NULL; // memory error
        executorsize++;
    }
    executors[executorcount] = executor;
    executorcount++;
    return Py_None
}

static PyObject* info_task(PyObject* self, PyObject* args){
    unsigned long id;

    if(!PyArg_ParseTuple(args, "k", &id))
        return NULL;

    if(tasktable == NULL) // TODO throw error here
        return PyUnicode_FromFormat("DFK Uninitialized");

    if(id >= taskcount) // TODO throw error here
        return PyUnicode_FromFormat("Task unallocated");

    struct task task = tasktable[id];

    return PyUnicode_FromFormat("Task %lu -> state: %i; depcount: %lu; exec_label: %s; func_name: %s; time invoked: %i; join: %i", // TODO find how to print float
                                task.id, task.status, task.depcount, task.exec_label, task.func_name, (int)task.time_invoked, task.join);
}

/*
 * TODO When freeing a task will need to decrement the refernce counts
 * of the python objects taken as a argument
 */
static PyObject* submit(PyObject* self, PyObject* args){
    char* exec_label,* func_name;
    int join;
    double time_invoked;
    PyObject* future,* executor,* func,* fargs=NULL,* fkwargs=NULL,* exec_fu=NULL;

    if(!PyArg_ParseTuple(args, "ssdpOOO|OO", &exec_label, &func_name, &time_invoked, &join, &future, &executor, &func, &fargs, &fkwargs))
        return NULL;

    if(appendtask(exec_label, func_name, time_invoked, join, future, executor, func, fargs, fkwargs) < 0)
        return NULL;

    // invoke executor submit function
    if(fargs != NULL){
        if(fkwargs != NULL)
            exec_fu = PyObject_CallMethodObjArgs(executor, pystr_submit, func, Py_None, fargs, fkwargs);
        else
            exec_fu = PyObject_CallMethodObjArgs(executor, pystr_submit, func, Py_None, fargs, NULL);
    }
    else{
        if(fkwargs != NULL)
            exec_fu = PyObject_CallMethodObjArgs(executor, pystr_submit, func, Py_None, Py_None, fkwargs, NULL);
        else
            exec_fu = PyObject_CallMethodObjArgs(executor, pystr_submit, func, Py_None, NULL);
    }

    if(exec_fu == NULL) // this exist because in the future we should set PyErr and return
        return NULL;

    return exec_fu;
}

char init_dfk_docs[] = "This method will initialize the dfk. In doing so this method will allocate memory for the dag and reset global state.";
char dest_dfk_docs[] = "This method will destroy the dfk. In doing so this method will dealocate memory for the dag and reset global state.";
char info_dfk_docs[] = "This method prints the global state associated with the dfk.";
char add_executor_dfk_docs[] = "This method appends a new executor to the executor table";
char submit_docs[] = "Takes in a function and its arguments, creates a task in the dag, and invokes executor.submit";
char info_task_docs[] = "takes as input an id as an int and returns information about a the task with that id";

PyMethodDef cdflow_funcs[] = {
    {"init_dfk", (PyCFunction)init_dfk, METH_VARARGS, init_dfk_docs},
    {"dest_dfk", (PyCFunction)dest_dfk, METH_NOARGS, dest_dfk_docs},
    {"info_dfk", (PyCFunction)info_dfk, METH_NOARGS, info_dfk_docs},
    {"info_task", (PyCFunction)info_task, METH_VARARGS, info_task_docs},
    {"add_executor_dfk", (PyCFunction)add_executor_dfk, METH_VARARGS, add_executor_dfk_docs},
    {"submit", (PyCFunction)submit, METH_VARARGS, submit_docs},
    {NULL}
};

char cdflow_docs[] = "Implementing the DFK in C";

PyModuleDef cdflow_mod = {
    PyModuleDef_HEAD_INIT,
    "backend",
    cdflow_docs,
    -1, // all per interpreter state is global, as a consequence this module cannot support sub interpreters
    cdflow_funcs,
    NULL,
    NULL,
    NULL,
    NULL
};

PyMODINIT_FUNC PyInit_cdflow(void){
    return PyModule_Create(&cdflow_mod);
}
