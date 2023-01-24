#include "dflow.h"

char method_docs[] = "this is a test method";

PyMethodDef cdfk_funcs[] = {
    {"method", (PyCFunction)method, METH_VARARGS, method_docs},
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
