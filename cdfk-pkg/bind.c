#include "dflow.h"

char init_dfk_docs[] = "The method will initialize the dfk. In doing so this method will allocate memory for the dag, and set global variables necessary for the dag.";
char dest_dfk_docs[] = "The method will destroy to dfk. In doing so this method will deallocate memory for the dag and reset global state.";
char info_dfk_docs[] = "The method prints the global state associated with the dfk.";
char submit_docs[] = "TODO Implement. Will be responsible for submiting task to the dfk";

char info_task_docs[] = "Takes as input an id as an int and returns the information about that task";

PyMethodDef cdfk_funcs[] = {
    {"init_dfk", (PyCFunction)init_dfk, METH_VARARGS, init_dfk_docs},
    {"dest_dfk", (PyCFunction)dest_dfk, METH_NOARGS, dest_dfk_docs},
    {"info_dfk", (PyCFunction)info_dfk, METH_NOARGS, info_dfk_docs},
    {"info_task", (PyCFunction)info_task, METH_VARARGS, info_task_docs},
    {"submit", (PyCFunction)submit, METH_VARARGS, submit_docs},
    {NULL}
};

char cdfk_docs[] = "Implementing the DFK in C";

PyModuleDef cdfk_mod = {
    PyModuleDef_HEAD_INIT,
    "cdfk",
    cdfk_docs,
    -1, // all per interpreter state is global
    cdfk_funcs,
    NULL,
    NULL,
    NULL,
    NULL
};

PyMODINIT_FUNC PyInit_cdfk(void){
    return PyModule_Create(&cdfk_mod);
}
