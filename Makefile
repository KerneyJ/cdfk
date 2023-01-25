
all:
	pip uninstall cdfk
	python setup.py build
	python setup.py install
